//! One federated peer: a `goose roam bridge <target>` child driven as an ACP client.
//!
//! Why a subprocess rather than dialing iroh in-process: `roam bridge` already does the
//! dial, the handshake and the trust check, and its stdio path is documented as ACP-clean
//! (only the splice touches stdout — see the roaming crate's bridge notes). Dialing here
//! would duplicate all of that to save one fork, so it stays a child until the process
//! cost is the thing that hurts.
//!
//! The connection runs on its own OS thread with a current-thread runtime, mirroring
//! `acp::provider`. That is not decoration: the ACP connection future is not `Send`, so it
//! cannot live on the shared tokio pool.

use crate::acp::custom_requests::{
    ArchiveSessionRequest, DeleteSessionRequest, EmptyResponse, RenameSessionRequest,
};
use agent_client_protocol::schema::v1::{
    CancelNotification, ClientCapabilities, InitializeRequest, InitializeResponse,
    ListSessionsRequest, ListSessionsResponse, LoadSessionRequest, LoadSessionResponse,
    PromptRequest, PromptResponse, RequestPermissionRequest, SessionId, SessionNotification,
    SetSessionConfigOptionRequest, SetSessionConfigOptionResponse,
};
use agent_client_protocol::schema::ProtocolVersion;
use agent_client_protocol::{Agent, Client, ConnectionTo};
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::process::Command;
use tokio::sync::{mpsc, oneshot, Mutex as TokioMutex};
use tokio_util::compat::{TokioAsyncReadCompatExt as _, TokioAsyncWriteCompatExt as _};
use tracing::{debug, info, warn};

use super::{federated_id, PeerEvent};

type AcpResult<T> = Result<T, agent_client_protocol::Error>;

/// Reconnect backoff. A peer that is simply switched off must not spin.
const RECONNECT_MIN: Duration = Duration::from_secs(2);
const RECONNECT_MAX: Duration = Duration::from_secs(60);

/// A request routed to a peer. Only the methods v1 federates appear here; everything else
/// is rejected up front in the server rather than half-forwarded.
pub(super) enum PeerCall {
    ListSessions(
        ListSessionsRequest,
        oneshot::Sender<AcpResult<ListSessionsResponse>>,
    ),
    LoadSession(
        LoadSessionRequest,
        oneshot::Sender<AcpResult<LoadSessionResponse>>,
    ),
    Prompt(PromptRequest, oneshot::Sender<AcpResult<PromptResponse>>),
    Cancel(CancelNotification),
    SetConfigOption(
        SetSessionConfigOptionRequest,
        oneshot::Sender<AcpResult<SetSessionConfigOptionResponse>>,
    ),
    RenameSession(
        RenameSessionRequest,
        oneshot::Sender<AcpResult<EmptyResponse>>,
    ),
    ArchiveSession(
        ArchiveSessionRequest,
        oneshot::Sender<AcpResult<EmptyResponse>>,
    ),
    DeleteSession(
        DeleteSessionRequest,
        oneshot::Sender<AcpResult<EmptyResponse>>,
    ),
}

/// A handle to one federated peer. Cheap to clone-by-reference; the connection itself lives
/// on a dedicated thread and survives client connects/disconnects.
pub(super) struct Peer {
    pub(super) name: String,
    calls: mpsc::Sender<PeerCall>,
    online: Arc<AtomicBool>,
}

impl Peer {
    /// Start supervising a peer. Returns immediately; the first connection attempt happens
    /// in the background, so a peer that is offline at boot does not delay server start.
    pub(super) fn spawn(name: String, target: String, events: mpsc::Sender<PeerEvent>) -> Self {
        let (tx, rx) = mpsc::channel(32);
        let online = Arc::new(AtomicBool::new(false));

        {
            let name = name.clone();
            let online = online.clone();
            let rx = Arc::new(TokioMutex::new(rx));
            std::thread::spawn(move || {
                let rt = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(rt) => rt,
                    Err(error) => {
                        warn!(peer = %name, %error, "failed to build roam peer runtime");
                        return;
                    }
                };
                rt.block_on(supervise(name, target, rx, events, online));
            });
        }

        Self {
            name,
            calls: tx,
            online,
        }
    }

    pub(super) fn is_online(&self) -> bool {
        self.online.load(Ordering::SeqCst)
    }

    async fn call<T>(
        &self,
        make: impl FnOnce(oneshot::Sender<AcpResult<T>>) -> PeerCall,
    ) -> AcpResult<T> {
        if !self.is_online() {
            return Err(offline(&self.name));
        }
        let (tx, rx) = oneshot::channel();
        self.calls
            .send(make(tx))
            .await
            .map_err(|_| offline(&self.name))?;
        rx.await.map_err(|_| offline(&self.name))?
    }

    pub(super) async fn list_sessions(
        &self,
        req: ListSessionsRequest,
    ) -> AcpResult<ListSessionsResponse> {
        self.call(|tx| PeerCall::ListSessions(req, tx)).await
    }

    pub(super) async fn load_session(
        &self,
        req: LoadSessionRequest,
    ) -> AcpResult<LoadSessionResponse> {
        self.call(|tx| PeerCall::LoadSession(req, tx)).await
    }

    pub(super) async fn prompt(&self, req: PromptRequest) -> AcpResult<PromptResponse> {
        self.call(|tx| PeerCall::Prompt(req, tx)).await
    }

    pub(super) async fn set_config_option(
        &self,
        req: SetSessionConfigOptionRequest,
    ) -> AcpResult<SetSessionConfigOptionResponse> {
        self.call(|tx| PeerCall::SetConfigOption(req, tx)).await
    }

    pub(super) async fn rename_session(
        &self,
        req: RenameSessionRequest,
    ) -> AcpResult<EmptyResponse> {
        self.call(|tx| PeerCall::RenameSession(req, tx)).await
    }

    pub(super) async fn archive_session(
        &self,
        req: ArchiveSessionRequest,
    ) -> AcpResult<EmptyResponse> {
        self.call(|tx| PeerCall::ArchiveSession(req, tx)).await
    }

    pub(super) async fn delete_session(
        &self,
        req: DeleteSessionRequest,
    ) -> AcpResult<EmptyResponse> {
        self.call(|tx| PeerCall::DeleteSession(req, tx)).await
    }

    /// Cancel is a notification: fire-and-forget, and silently dropped when the peer is
    /// offline (there is nothing running there to cancel).
    pub(super) fn cancel(&self, notif: CancelNotification) {
        if self.is_online() {
            let _ = self.calls.try_send(PeerCall::Cancel(notif));
        }
    }
}

pub(super) fn offline(peer: &str) -> agent_client_protocol::Error {
    agent_client_protocol::Error::internal_error()
        .data(format!("roam peer `{peer}` is not connected"))
}

/// Connect, serve calls until the connection dies, then back off and try again — forever.
/// A peer being down is an expected steady state, not an error to give up on.
async fn supervise(
    name: String,
    target: String,
    calls: Arc<TokioMutex<mpsc::Receiver<PeerCall>>>,
    events: mpsc::Sender<PeerEvent>,
    online: Arc<AtomicBool>,
) {
    let mut backoff = RECONNECT_MIN;
    loop {
        match connect_once(&name, &target, &calls, &events, &online).await {
            Ok(()) => {
                debug!(peer = %name, "roam peer connection closed cleanly");
                backoff = RECONNECT_MIN;
            }
            Err(error) => {
                warn!(peer = %name, %error, "roam peer connection failed");
            }
        }
        online.store(false, Ordering::SeqCst);
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(RECONNECT_MAX);
    }
}

async fn connect_once(
    name: &str,
    target: &str,
    calls: &Arc<TokioMutex<mpsc::Receiver<PeerCall>>>,
    events: &mpsc::Sender<PeerEvent>,
    online: &Arc<AtomicBool>,
) -> anyhow::Result<()> {
    // current_exe, not "goose" on PATH: in the container and in a dev build the binary is
    // not necessarily on PATH under that name, and silently bridging to a *different*
    // goose build would be worse than failing.
    let exe = std::env::current_exe()?;
    let mut child = Command::new(exe)
        .arg("roam")
        .arg("bridge")
        .arg(target)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;

    let stdin = child
        .stdin
        .take()
        .ok_or_else(|| anyhow::anyhow!("bridge child has no stdin"))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| anyhow::anyhow!("bridge child has no stdout"))?;
    if let Some(stderr) = child.stderr.take() {
        // `bridge` reports dial/handshake status on stderr; it is the only place a
        // "peer not accepted" or "peer offline" reason ever appears.
        let name = name.to_string();
        tokio::spawn(forward_stderr(name, stderr));
    }

    let transport = agent_client_protocol::ByteStreams::new(stdin.compat_write(), stdout.compat());

    let result = run_connection(name, target, calls, events, online, transport).await;

    let _ = child.kill().await;
    let _ = child.wait().await;
    result
}

async fn run_connection(
    name: &str,
    target: &str,
    calls: &Arc<TokioMutex<mpsc::Receiver<PeerCall>>>,
    events: &mpsc::Sender<PeerEvent>,
    online: &Arc<AtomicBool>,
    transport: impl agent_client_protocol::ConnectTo<Client> + 'static,
) -> anyhow::Result<()> {
    let notify_name = name.to_string();
    let notify_events = events.clone();
    let perm_name = name.to_string();
    let perm_events = events.clone();

    let loop_name = name.to_string();
    let loop_target = target.to_string();
    let loop_calls = calls.clone();
    let loop_online = online.clone();

    Client
        .builder()
        .on_receive_notification(
            {
                async move |mut notification: SessionNotification, _cx| {
                    // The remote knows its own session ids and nothing else. Rewriting here,
                    // at the boundary, is what lets every handler above this point treat a
                    // federated id as just another opaque string.
                    notification.session_id = SessionId::new(federated_id(
                        &notify_name,
                        notification.session_id.0.as_ref(),
                    ));
                    let _ = notify_events.try_send(PeerEvent::Notification(Box::new(notification)));
                    Ok(())
                }
            },
            agent_client_protocol::on_receive_notification!(),
        )
        .on_receive_request(
            {
                async move |mut request: RequestPermissionRequest, responder, _cx| {
                    request.session_id =
                        SessionId::new(federated_id(&perm_name, request.session_id.0.as_ref()));
                    let (tx, rx) = oneshot::channel();
                    if perm_events
                        .try_send(PeerEvent::Permission(Box::new(request), tx))
                        .is_err()
                    {
                        // Nobody is listening — deny rather than hang the remote agent.
                        return responder.respond(super::cancelled_permission());
                    }
                    let response = rx.await.unwrap_or_else(|_| super::cancelled_permission());
                    responder.respond(response)
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .connect_with(transport, async move |cx: ConnectionTo<Agent>| {
            let _init: InitializeResponse = cx
                .send_request(
                    InitializeRequest::new(ProtocolVersion::LATEST)
                        .client_capabilities(ClientCapabilities::new()),
                )
                .block_task()
                .await?;

            info!(peer = %loop_name, target = %loop_target, "roam peer connected");
            loop_online.store(true, Ordering::SeqCst);

            // Only this task ever consumes the queue, so holding the lock for the life of
            // the connection is intentional rather than contended.
            // Each request runs as its own spawned task: the jsonrpc layer multiplexes by
            // id, so nothing forces serialization — and awaiting inline here did exactly
            // that, parking a session/list behind a minutes-long remote prompt until the
            // caller's timeout wrote the peer off as dead.
            macro_rules! forward {
                ($req:expr, $tx:expr) => {{
                    let (req, tx) = ($req, $tx);
                    let cx2 = cx.clone();
                    cx.spawn(async move {
                        let _ = tx.send(cx2.send_request(req).block_task().await);
                        Ok(())
                    })?;
                }};
            }
            let mut calls = loop_calls.lock().await;
            while let Some(call) = calls.recv().await {
                match call {
                    PeerCall::ListSessions(req, tx) => forward!(req, tx),
                    PeerCall::LoadSession(req, tx) => forward!(req, tx),
                    PeerCall::Prompt(req, tx) => forward!(req, tx),
                    PeerCall::SetConfigOption(req, tx) => forward!(req, tx),
                    PeerCall::RenameSession(req, tx) => forward!(req, tx),
                    PeerCall::ArchiveSession(req, tx) => forward!(req, tx),
                    PeerCall::DeleteSession(req, tx) => forward!(req, tx),
                    PeerCall::Cancel(notif) => {
                        let _ = cx.send_notification(notif);
                    }
                }
            }
            Ok(())
        })
        .await?;

    Ok(())
}

/// Forwards the bridge child's stderr into tracing, line by line and length-capped so a
/// child that never emits a newline cannot grow the buffer without bound.
async fn forward_stderr(name: String, mut stderr: tokio::process::ChildStderr) {
    use tokio::io::AsyncReadExt as _;

    const MAX_LINE_LEN: usize = 4096;
    let mut line: Vec<u8> = Vec::with_capacity(256);
    let mut chunk = [0u8; 1024];
    loop {
        match stderr.read(&mut chunk).await {
            Ok(0) => break,
            Ok(n) => {
                for &b in &chunk[..n] {
                    if b == b'\n' || line.len() >= MAX_LINE_LEN {
                        if !line.is_empty() {
                            debug!(peer = %name, "roam bridge: {}", String::from_utf8_lossy(&line));
                            line.clear();
                        }
                        if b != b'\n' {
                            line.push(b);
                        }
                    } else {
                        line.push(b);
                    }
                }
            }
            Err(_) => break,
        }
    }
    if !line.is_empty() {
        debug!(peer = %name, "roam bridge: {}", String::from_utf8_lossy(&line));
    }
}
