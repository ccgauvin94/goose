//! Federating remote roam peers into this server's ACP surface.
//!
//! `goose roam` moves ACP bytes and has no notion of a session. This module is what turns
//! "a peer I can dial" into "sessions that show up in every client connected to this
//! server" — Desktop, the CLI, a phone — without any of them learning what roam is. The
//! alternative is each client growing its own peer picker, which is the same work done N
//! times and diverging N ways.
//!
//! ## The id is the whole trick
//!
//! An ACP session id is an opaque string (`server.rs`: "the ACP session_id IS the thread
//! ID"), so a federated session is addressed as `roam:<peer>:<remote id>`. Rewriting at the
//! two boundaries — outbound in `merge_sessions`, inbound in the peer's notification
//! handler — means every handler in between treats it as just another id.
//!
//! ## What federates, and what it refuses
//!
//! Routed: `session/list`, `session/load`, `session/prompt`, `session/cancel`,
//! `session/set_config_option`, rename/archive/delete, the session-scoped tool and
//! extension methods (`tools/list`, `session/extensions/list|add|remove`), and
//! permission requests coming back the other way. set_config_option is sound to route
//! because the options the client picks from came from the remote via `session/load` —
//! the write goes back to the node that offered the choices. The same argument covers
//! extensions/add: a client toggling remote tools round-trips extension objects it got
//! from the remote's own extensions/list. Everything else — fork, close, steer, and the
//! global (non-session-scoped) config methods — is refused with a clear error for a
//! federated id rather than being half-forwarded.
//!
//! ## Pagination is deliberately shallow
//!
//! Remote peers contribute their first page only, and only to the client's first page
//! (`cursor: None`). Merging N independently-cursored sources needs a composite cursor
//! carrying a position per peer; until that exists, a peer with more than one page of
//! sessions has its tail invisible. `merge_sessions` logs when it truncates, because a
//! silently short list reads exactly like a peer with few sessions.

mod peer;

use crate::acp::custom_requests::{
    AddSessionExtensionRequest, ArchiveSessionRequest, DeleteSessionRequest, EmptyResponse,
    GetSessionExtensionsRequest, GetSessionExtensionsResponse, GetToolsRequest, GetToolsResponse,
    RemoveSessionExtensionRequest, RenameSessionRequest,
};
use agent_client_protocol::schema::v1::{
    CancelNotification, ListSessionsRequest, ListSessionsResponse, LoadSessionRequest,
    LoadSessionResponse, PromptRequest, PromptResponse, RequestPermissionOutcome,
    RequestPermissionRequest, RequestPermissionResponse, SessionId, SessionInfo,
    SessionNotification, SetSessionConfigOptionRequest, SetSessionConfigOptionResponse,
};
use agent_client_protocol::{Client, ConnectionTo};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tracing::{info, warn};

use peer::Peer;

/// Marks a session id as living on a remote peer. Local ids are opaque strings but none of
/// goose's generators produce this prefix, so it cannot collide with a real local session.
const FEDERATED_PREFIX: &str = "roam:";

/// How many events can queue between the peer connections and the connected client before
/// the oldest are dropped. Notifications are advisory; a client that cannot keep up would
/// rather lose stream chunks than stall the remote agent.
const EVENT_QUEUE: usize = 256;

/// How long a peer gets to answer session/list before it is treated as offline for this
/// merge. `is_online()` is necessary but not sufficient: a share restart on the remote can
/// leave the bridge connection dead without the child noticing, and an unbounded await here
/// wedged EVERY client's session/list at once (observed 2026-08-06, recovered only by
/// restarting serve). A slow peer losing its page beats one stale peer freezing the server.
const PEER_LIST_TIMEOUT: Duration = Duration::from_secs(5);

/// session/load replays a whole transcript, so it gets more room than list — but the same
/// dead-connection hang applies, and a client stuck on load looks identical to a hung app.
const PEER_LOAD_TIMEOUT: Duration = Duration::from_secs(30);

/// rename/archive/delete are single-row session-store writes on the remote, and the tool/
/// extension calls are in-memory lookups plus at most one extension (re)start; ten seconds
/// is generous for anything but a dead connection.
const PEER_MANAGE_TIMEOUT: Duration = Duration::from_secs(10);

/// Builds the id a client sees for a session that lives on `peer`.
pub(super) fn federated_id(peer: &str, remote: &str) -> String {
    format!("{FEDERATED_PREFIX}{peer}:{remote}")
}

/// Splits a client-facing id back into `(peer, remote id)`, or `None` if it is local.
pub(super) fn split_federated_id(id: &str) -> Option<(&str, &str)> {
    let rest = id.strip_prefix(FEDERATED_PREFIX)?;
    let (peer, remote) = rest.split_once(':')?;
    if peer.is_empty() || remote.is_empty() {
        return None;
    }
    Some((peer, remote))
}

fn cancelled_permission() -> RequestPermissionResponse {
    RequestPermissionResponse::new(RequestPermissionOutcome::Cancelled)
}

/// Something a peer connection needs the locally-connected client to handle.
enum PeerEvent {
    Notification(Box<SessionNotification>),
    Permission(
        Box<RequestPermissionRequest>,
        oneshot::Sender<RequestPermissionResponse>,
    ),
}

/// The peer pool. One per server process, shared by every client connection — peers must
/// not be redialled each time a client reconnects.
pub struct Federation {
    peers: Vec<Peer>,
    /// The client currently being served. Last connection wins: with two clients attached,
    /// remote notifications follow the newer one. Fixing that means routing by which
    /// connection loaded the session, which v1 does not track.
    client: Arc<Mutex<Option<ConnectionTo<Client>>>>,
}

impl Federation {
    /// Reads the peer list and starts supervising each one. Returns `None` when nothing is
    /// configured, so the whole feature costs a null check when unused.
    ///
    /// Peers come from `GOOSE_ROAM_FEDERATE` (comma-separated) or the `roam_federate` key
    /// in config.yaml. Each entry is a `roam peers` nickname or a `goose+roam://` card.
    pub fn from_config() -> Option<Arc<Self>> {
        let targets = configured_targets();
        if targets.is_empty() {
            return None;
        }
        Some(Self::new(targets))
    }

    fn new(targets: Vec<String>) -> Arc<Self> {
        let (tx, rx) = mpsc::channel(EVENT_QUEUE);
        let client: Arc<Mutex<Option<ConnectionTo<Client>>>> = Arc::new(Mutex::new(None));

        let peers = targets
            .into_iter()
            .filter_map(|target| {
                let name = peer_name(&target)?;
                info!(peer = %name, %target, "federating roam peer");
                Some(Peer::spawn(name, target, tx.clone()))
            })
            .collect();

        tokio::spawn(pump(rx, client.clone()));

        Arc::new(Self { peers, client })
    }

    /// Point the pump at the client that just connected. Called once per ACP connection.
    pub fn register_client(&self, cx: ConnectionTo<Client>) {
        if let Ok(mut guard) = self.client.lock() {
            *guard = Some(cx);
        }
    }

    fn peer(&self, name: &str) -> Result<&Peer, agent_client_protocol::Error> {
        self.peers
            .iter()
            .find(|peer| peer.name == name)
            .ok_or_else(|| {
                agent_client_protocol::Error::resource_not_found(Some(format!(
                    "roam peer `{name}` is not federated by this server"
                )))
            })
    }

    /// Loads a session that lives on `peer`, rewriting the id to the one the remote knows.
    pub(super) async fn load_session(
        &self,
        peer: &str,
        remote_id: &str,
        mut req: LoadSessionRequest,
    ) -> Result<LoadSessionResponse, agent_client_protocol::Error> {
        req.session_id = SessionId::new(remote_id.to_string());
        match tokio::time::timeout(PEER_LOAD_TIMEOUT, self.peer(peer)?.load_session(req)).await {
            Ok(result) => result,
            Err(_) => {
                warn!(peer = %peer, %remote_id, "roam peer session load timed out");
                Err(peer::offline(peer))
            }
        }
    }

    pub(super) async fn prompt(
        &self,
        peer: &str,
        remote_id: &str,
        mut req: PromptRequest,
    ) -> Result<PromptResponse, agent_client_protocol::Error> {
        req.session_id = SessionId::new(remote_id.to_string());
        self.peer(peer)?.prompt(req).await
    }

    pub(super) fn cancel(&self, peer: &str, remote_id: &str, mut notif: CancelNotification) {
        notif.session_id = SessionId::new(remote_id.to_string());
        if let Ok(peer) = self.peer(peer) {
            peer.cancel(notif);
        }
    }

    /// Sets a config knob on the remote session. Sound because the choices the client is
    /// picking from came from the remote in the first place: session/load returns the
    /// REMOTE's configOptions for a federated session, so the write goes back to the node
    /// that offered them. The response is the remote's refreshed configOptions, passed
    /// through unchanged (it carries no session ids to rewrite).
    pub(super) async fn set_config_option(
        &self,
        peer: &str,
        remote_id: &str,
        mut req: SetSessionConfigOptionRequest,
    ) -> Result<SetSessionConfigOptionResponse, agent_client_protocol::Error> {
        req.session_id = SessionId::new(remote_id.to_string());
        match tokio::time::timeout(PEER_LOAD_TIMEOUT, self.peer(peer)?.set_config_option(req)).await
        {
            Ok(result) => result,
            Err(_) => {
                warn!(peer = %peer, %remote_id, "roam peer set_config_option timed out");
                Err(peer::offline(peer))
            }
        }
    }

    pub(super) async fn rename_session(
        &self,
        peer: &str,
        remote_id: &str,
        mut req: RenameSessionRequest,
    ) -> Result<EmptyResponse, agent_client_protocol::Error> {
        req.session_id = remote_id.to_string();
        self.manage(peer, "rename", self.peer(peer)?.rename_session(req))
            .await
    }

    pub(super) async fn archive_session(
        &self,
        peer: &str,
        remote_id: &str,
        mut req: ArchiveSessionRequest,
    ) -> Result<EmptyResponse, agent_client_protocol::Error> {
        req.session_id = remote_id.to_string();
        self.manage(peer, "archive", self.peer(peer)?.archive_session(req))
            .await
    }

    pub(super) async fn delete_session(
        &self,
        peer: &str,
        remote_id: &str,
        mut req: DeleteSessionRequest,
    ) -> Result<EmptyResponse, agent_client_protocol::Error> {
        req.session_id = remote_id.to_string();
        self.manage(peer, "delete", self.peer(peer)?.delete_session(req))
            .await
    }

    /// Lists the tools active in the remote session. Read-only; the response carries no
    /// session ids to rewrite.
    pub(super) async fn list_tools(
        &self,
        peer: &str,
        remote_id: &str,
        mut req: GetToolsRequest,
    ) -> Result<GetToolsResponse, agent_client_protocol::Error> {
        req.session_id = remote_id.to_string();
        self.manage(peer, "tools/list", self.peer(peer)?.list_tools(req))
            .await
    }

    /// Lists the extensions attached to the remote session, as the REMOTE's own extension
    /// objects — which is what makes add_session_extension below sound: a client editing a
    /// remote session's tool allowlist round-trips objects it got from here.
    pub(super) async fn list_session_extensions(
        &self,
        peer: &str,
        remote_id: &str,
        mut req: GetSessionExtensionsRequest,
    ) -> Result<GetSessionExtensionsResponse, agent_client_protocol::Error> {
        req.session_id = remote_id.to_string();
        self.manage(
            peer,
            "extensions/list",
            self.peer(peer)?.list_session_extensions(req),
        )
        .await
    }

    /// Session-scoped on the remote: touches that session's extension_data, never the
    /// peer's config.yaml. The extension object should originate from the peer's own
    /// extensions/list — a local extension config can name commands that don't exist
    /// there, and the remote will report that failure itself.
    pub(super) async fn add_session_extension(
        &self,
        peer: &str,
        remote_id: &str,
        mut req: AddSessionExtensionRequest,
    ) -> Result<EmptyResponse, agent_client_protocol::Error> {
        req.session_id = remote_id.to_string();
        self.manage(
            peer,
            "extensions/add",
            self.peer(peer)?.add_session_extension(req),
        )
        .await
    }

    pub(super) async fn remove_session_extension(
        &self,
        peer: &str,
        remote_id: &str,
        mut req: RemoveSessionExtensionRequest,
    ) -> Result<EmptyResponse, agent_client_protocol::Error> {
        req.session_id = remote_id.to_string();
        self.manage(
            peer,
            "extensions/remove",
            self.peer(peer)?.remove_session_extension(req),
        )
        .await
    }

    /// Shared timeout wrapper for the quick session-scoped management calls.
    async fn manage<T>(
        &self,
        peer: &str,
        operation: &str,
        call: impl std::future::Future<Output = Result<T, agent_client_protocol::Error>>,
    ) -> Result<T, agent_client_protocol::Error> {
        match tokio::time::timeout(PEER_MANAGE_TIMEOUT, call).await {
            Ok(result) => result,
            Err(_) => {
                warn!(peer = %peer, %operation, "roam peer session management call timed out");
                Err(peer::offline(peer))
            }
        }
    }

    /// Appends every online peer's sessions to a locally-produced page.
    ///
    /// Only runs on the first page: re-appending remote sessions to page 2 would duplicate
    /// every one of them. The local `next_cursor` is passed through untouched, so local
    /// pagination keeps working exactly as before.
    pub(super) async fn merge_sessions(
        &self,
        req: &ListSessionsRequest,
        mut local: ListSessionsResponse,
    ) -> ListSessionsResponse {
        if req.cursor.is_some() {
            return local;
        }

        for peer in &self.peers {
            if !peer.is_online() {
                // Not an error: a peer that is switched off must shorten the list, never
                // fail it, or one dead machine breaks every client's session list.
                continue;
            }

            let mut remote_req = ListSessionsRequest::new();
            remote_req.cwd = req.cwd.clone();
            remote_req.meta = req.meta.clone();

            match tokio::time::timeout(PEER_LIST_TIMEOUT, peer.list_sessions(remote_req)).await {
                Err(_) => {
                    warn!(
                        peer = %peer.name,
                        "roam peer session list timed out; omitting its sessions from this page"
                    );
                }
                Ok(Ok(response)) => {
                    if response.next_cursor.is_some() {
                        warn!(
                            peer = %peer.name,
                            "roam peer has more sessions than one page; the tail is not \
                             listed (federated pagination is first-page-only)"
                        );
                    }
                    local.sessions.extend(
                        response
                            .sessions
                            .into_iter()
                            .map(|info| rewrite(&peer.name, info)),
                    );
                }
                Ok(Err(error)) => {
                    warn!(peer = %peer.name, ?error, "roam peer session list failed");
                }
            }
        }

        local.sessions.sort_by(|a, b| {
            sort_key(b.updated_at.as_deref()).cmp(&sort_key(a.updated_at.as_deref()))
        });
        local
    }
}

/// Re-addresses one remote session so the client can round-trip it back to us.
fn rewrite(peer: &str, mut info: SessionInfo) -> SessionInfo {
    info.session_id = SessionId::new(federated_id(peer, info.session_id.0.as_ref()));
    info
}

/// Sorts on a parsed instant, not the raw string: `updated_at` is RFC 3339, which may carry
/// a non-UTC offset, and lexical order across offsets is wrong.
fn sort_key(updated_at: Option<&str>) -> Option<chrono::DateTime<chrono::Utc>> {
    updated_at
        .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok())
        .map(|dt| dt.with_timezone(&chrono::Utc))
}

/// Forwards peer events to whichever client is currently attached.
async fn pump(mut rx: mpsc::Receiver<PeerEvent>, client: Arc<Mutex<Option<ConnectionTo<Client>>>>) {
    while let Some(event) = rx.recv().await {
        // Clone the handle out before any await: the guard must not be held across one.
        let cx = client.lock().ok().and_then(|guard| guard.clone());
        match event {
            PeerEvent::Notification(notification) => {
                let Some(cx) = cx else { continue };
                if let Err(error) = cx.send_notification(*notification) {
                    warn!(%error, "failed to forward roam peer notification");
                }
            }
            PeerEvent::Permission(request, responder) => {
                // Spawned: a permission prompt waits on a human, and the pump must keep
                // delivering stream chunks for the very session being asked about.
                tokio::spawn(async move {
                    let response = match cx {
                        Some(cx) => cx
                            .send_request(*request)
                            .block_task()
                            .await
                            .unwrap_or_else(|_| cancelled_permission()),
                        None => cancelled_permission(),
                    };
                    let _ = responder.send(response);
                });
            }
        }
    }
}

fn configured_targets() -> Vec<String> {
    if let Ok(raw) = std::env::var("GOOSE_ROAM_FEDERATE") {
        let targets = split_targets(&raw);
        if !targets.is_empty() {
            return targets;
        }
    }
    crate::config::Config::global()
        .get_param::<Vec<String>>("roam_federate")
        .unwrap_or_default()
        .into_iter()
        .map(|t| t.trim().to_string())
        .filter(|t| !t.is_empty())
        .collect()
}

fn split_targets(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(|t| t.trim().to_string())
        .filter(|t| !t.is_empty())
        .collect()
}

/// The name a peer is known by in federated ids.
///
/// A nickname is used as-is. A `goose+roam://` card has no nickname, so it is refused:
/// deriving one from the card would produce an id that changes if the card is re-pasted in
/// another form, and session ids must be stable.
fn peer_name(target: &str) -> Option<String> {
    if target.contains("://") {
        warn!(
            %target,
            "roam federation needs a saved peer nickname, not a card; run \
             `goose roam peers accept <card> <name>` first"
        );
        return None;
    }
    if target.contains(':') {
        warn!(%target, "roam peer nickname must not contain ':'");
        return None;
    }
    Some(target.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_a_federated_id() {
        let id = federated_id("workstation", "20260805_120000");
        assert_eq!(id, "roam:workstation:20260805_120000");
        assert_eq!(
            split_federated_id(&id),
            Some(("workstation", "20260805_120000"))
        );
    }

    #[test]
    fn local_ids_are_not_federated() {
        assert_eq!(split_federated_id("20260805_120000"), None);
        assert_eq!(split_federated_id("roam:"), None);
        assert_eq!(split_federated_id("roam:peer:"), None);
        assert_eq!(split_federated_id("roam::abc"), None);
    }

    #[test]
    fn remote_ids_may_contain_colons() {
        // Only the FIRST colon separates peer from remote id, so a remote id that itself
        // contains one survives the round trip.
        let id = federated_id("peer", "a:b:c");
        assert_eq!(split_federated_id(&id), Some(("peer", "a:b:c")));
    }

    #[test]
    fn cards_and_colon_names_are_rejected() {
        assert_eq!(peer_name("workstation"), Some("workstation".to_string()));
        assert_eq!(peer_name("goose+roam://abcdef"), None);
        assert_eq!(peer_name("host:1234"), None);
    }

    #[test]
    fn targets_split_on_commas_and_drop_blanks() {
        assert_eq!(
            split_targets(" a , ,b,, c "),
            vec!["a".to_string(), "b".to_string(), "c".to_string()]
        );
    }

    #[test]
    fn sort_key_orders_across_offsets() {
        // 2026-08-05T00:30:00-06:00 is LATER than 2026-08-05T02:00:00Z, which a lexical
        // string sort gets backwards.
        let mountain = sort_key(Some("2026-08-05T00:30:00-06:00"));
        let utc = sort_key(Some("2026-08-05T02:00:00Z"));
        assert!(mountain > utc);
        assert_eq!(sort_key(Some("not a timestamp")), None);
        assert_eq!(sort_key(None), None);
    }

    fn session(id: &str, updated_at: &str) -> SessionInfo {
        SessionInfo::new(
            SessionId::new(id.to_string()),
            std::path::PathBuf::from("/"),
        )
        .updated_at(updated_at.to_string())
    }

    fn ids(response: &ListSessionsResponse) -> Vec<&str> {
        response
            .sessions
            .iter()
            .map(|info| info.session_id.0.as_ref())
            .collect()
    }

    #[test]
    fn rewrite_readdresses_a_remote_session() {
        let info = rewrite("workstation", session("abc", "2026-08-05T00:00:00Z"));
        assert_eq!(info.session_id.0.as_ref(), "roam:workstation:abc");
    }

    #[tokio::test]
    async fn merge_sorts_newest_first() {
        let federation = Federation::new(Vec::new());
        let local = ListSessionsResponse::new(vec![
            session("old", "2026-08-01T00:00:00Z"),
            session("new", "2026-08-05T00:00:00Z"),
            session("middle", "2026-08-03T00:00:00Z"),
        ]);

        let merged = federation
            .merge_sessions(&ListSessionsRequest::new(), local)
            .await;

        assert_eq!(ids(&merged), vec!["new", "middle", "old"]);
    }

    #[tokio::test]
    async fn merge_leaves_later_pages_alone() {
        // Re-appending remote sessions to every page would duplicate each of them once per
        // page, so page 2+ must come back exactly as the local store produced it —
        // including the order, which is the local cursor's order and not ours to change.
        let federation = Federation::new(Vec::new());
        let mut req = ListSessionsRequest::new();
        req.cursor = Some("opaque-local-cursor".to_string());
        let local = ListSessionsResponse::new(vec![
            session("old", "2026-08-01T00:00:00Z"),
            session("new", "2026-08-05T00:00:00Z"),
        ]);

        let merged = federation.merge_sessions(&req, local).await;

        assert_eq!(ids(&merged), vec!["old", "new"]);
    }

    #[tokio::test]
    async fn merge_passes_the_local_cursor_through() {
        let federation = Federation::new(Vec::new());
        let local = ListSessionsResponse::new(vec![session("a", "2026-08-01T00:00:00Z")])
            .next_cursor(Some("local-next".to_string()));

        let merged = federation
            .merge_sessions(&ListSessionsRequest::new(), local)
            .await;

        assert_eq!(merged.next_cursor.as_deref(), Some("local-next"));
    }

    #[tokio::test]
    async fn an_unknown_peer_is_named_rather_than_reported_missing() {
        let federation = Federation::new(Vec::new());
        let error = federation
            .load_session(
                "nosuch",
                "abc",
                LoadSessionRequest::new(SessionId::new("abc"), std::path::PathBuf::from("/")),
            )
            .await
            .expect_err("a peer that is not federated must not resolve");
        assert!(
            format!("{error:?}").contains("nosuch"),
            "error should name the peer, got: {error:?}"
        );
    }
}
