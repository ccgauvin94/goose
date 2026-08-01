use super::*;
use std::path::{Path, PathBuf};

/// Config key holding extra browsable roots, separated by the platform path separator
/// (`:` on unix). Empty or unset means "only the session's working directory".
const BROWSE_ROOTS_KEY: &str = "GOOSE_BROWSE_ROOTS";

/// Resolve a client-supplied path to what the OS would actually open.
///
/// Separate from [`is_within_roots`] only so the pair can be unit-tested; they must always be
/// used together and IN THIS ORDER. Checking containment on the unresolved string is the
/// classic escape: `<root>/../etc` and a symlink pointing out of a root both pass a textual
/// prefix test and then open something else entirely.
fn resolve_path(requested: &str) -> std::io::Result<PathBuf> {
    PathBuf::from(requested).canonicalize()
}

/// Containment test. `canonical` MUST already be canonicalised -- see [`resolve_path`].
fn is_within_roots(canonical: &Path, roots: &[PathBuf]) -> bool {
    roots.iter().any(|r| canonical.starts_with(r))
}

impl GooseAcpAgent {
    /// Roots this session may browse: its own working directory, plus anything operator-configured.
    ///
    /// Deliberately NOT the whole filesystem. goose runs with the user's privileges and, in a
    /// container deployment, that includes its own config directory -- `secrets.yaml` and every
    /// stored provider key. The ACP endpoint can be internet-reachable behind one shared
    /// credential, so an unrestricted lister would promote that credential into a remote
    /// filesystem read primitive. The working directory is included automatically because
    /// browsing the project you are working in needs no configuration to be useful.
    fn browse_roots(&self, session_working_dir: &Path) -> Vec<PathBuf> {
        let mut roots: Vec<PathBuf> = vec![session_working_dir.to_path_buf()];

        if let Ok(configured) = Config::global().get_param::<String>(BROWSE_ROOTS_KEY) {
            for part in configured.split(':').filter(|s| !s.trim().is_empty()) {
                roots.push(PathBuf::from(part.trim()));
            }
        }

        // Canonicalise so the containment test below compares like with like; drop roots that do
        // not resolve rather than keeping a path that can never match.
        roots
            .into_iter()
            .filter_map(|r| r.canonicalize().ok())
            .fold(Vec::new(), |mut acc, r| {
                if !acc.contains(&r) {
                    acc.push(r);
                }
                acc
            })
    }

    pub(super) async fn on_list_directory(
        &self,
        req: ListDirectoryRequest,
    ) -> Result<ListDirectoryResponse, agent_client_protocol::Error> {
        let session_id = &req.session_id;
        let session = self
            .session_manager
            .get_session(session_id, false)
            .await
            .map_err(|_| {
                agent_client_protocol::Error::resource_not_found(Some(session_id.to_string()))
                    .data(format!("Session not found: {}", session_id))
            })?;

        let roots = self.browse_roots(&session.working_dir);
        let root_strings: Vec<String> = roots
            .iter()
            .map(|r| r.to_string_lossy().to_string())
            .collect();

        // No path: enumerate the roots themselves. This is how a client discovers where it may
        // browse instead of hardcoding a guess and getting a permission error.
        let requested = req.path.as_deref().map(str::trim).unwrap_or("");
        if requested.is_empty() {
            let entries = roots
                .iter()
                .map(|r| DirectoryEntry {
                    name: r
                        .file_name()
                        .map(|n| n.to_string_lossy().to_string())
                        .unwrap_or_else(|| r.to_string_lossy().to_string()),
                    path: r.to_string_lossy().to_string(),
                    is_dir: true,
                    size: None,
                    modified: None,
                    is_symlink: false,
                })
                .collect();
            return Ok(ListDirectoryResponse {
                path: String::new(),
                parent: None,
                entries,
                roots: root_strings,
            });
        }

        let canonical = resolve_path(requested).map_err(|e| {
            agent_client_protocol::Error::resource_not_found(Some(requested.to_string()))
                .data(format!("Cannot resolve {}: {}", requested, e))
        })?;

        if !is_within_roots(&canonical, &roots) {
            // Say it is outside the allowlist rather than "not found": the path may well exist,
            // and pretending otherwise sends the user hunting for a typo that is not there. The
            // roots are returned to the client anyway, so this leaks nothing it cannot already see.
            return Err(agent_client_protocol::Error::invalid_params().data(format!(
                "Path is outside the browsable roots ({}): {}",
                root_strings.join(", "),
                canonical.display()
            )));
        }

        if !canonical.is_dir() {
            return Err(agent_client_protocol::Error::invalid_params()
                .data(format!("Not a directory: {}", canonical.display())));
        }

        let mut read_dir = tokio::fs::read_dir(&canonical)
            .await
            .map_err(|e| agent_client_protocol::Error::internal_error().data(e.to_string()))?;

        let mut entries: Vec<DirectoryEntry> = Vec::new();
        while let Some(item) = read_dir
            .next_entry()
            .await
            .map_err(|e| agent_client_protocol::Error::internal_error().data(e.to_string()))?
        {
            let entry_path = item.path();
            // symlink_metadata tells us it IS a link; metadata() follows it so is_dir/size
            // describe the target, which is what decides whether a browser row is enterable.
            let is_symlink = tokio::fs::symlink_metadata(&entry_path)
                .await
                .map(|m| m.file_type().is_symlink())
                .unwrap_or(false);
            let meta = tokio::fs::metadata(&entry_path).await.ok();
            let is_dir = meta.as_ref().map(|m| m.is_dir()).unwrap_or(false);
            let modified = meta
                .as_ref()
                .and_then(|m| m.modified().ok())
                .map(|t| chrono::DateTime::<chrono::Utc>::from(t).to_rfc3339());

            entries.push(DirectoryEntry {
                name: item.file_name().to_string_lossy().to_string(),
                path: entry_path.to_string_lossy().to_string(),
                is_dir,
                size: if is_dir {
                    None
                } else {
                    meta.as_ref().map(|m| m.len())
                },
                modified,
                is_symlink,
            });
        }

        entries.sort_by(|a, b| match b.is_dir.cmp(&a.is_dir) {
            std::cmp::Ordering::Equal => a.name.to_lowercase().cmp(&b.name.to_lowercase()),
            other => other,
        });

        // Only offer a parent while it stays inside an allowed root, so a client cannot walk out
        // one directory at a time.
        let parent = canonical
            .parent()
            .filter(|p| roots.iter().any(|r| p.starts_with(r)))
            .map(|p| p.to_string_lossy().to_string());

        Ok(ListDirectoryResponse {
            path: canonical.to_string_lossy().to_string(),
            parent,
            entries,
            roots: root_strings,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{is_within_roots, resolve_path};
    use std::path::PathBuf;

    fn tmp(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("goose-browse-test-{}", name));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d.canonicalize().unwrap()
    }

    #[test]
    fn plain_subdirectory_is_allowed() {
        let base = tmp("plain");
        let root = base.join("root");
        std::fs::create_dir_all(root.join("sub")).unwrap();
        let roots = vec![root.canonicalize().unwrap()];
        let p = resolve_path(root.join("sub").to_str().unwrap()).unwrap();
        assert!(is_within_roots(&p, &roots));
    }

    #[test]
    fn dotdot_cannot_escape_a_root() {
        let base = tmp("dotdot");
        let root = base.join("root");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::create_dir_all(base.join("outside")).unwrap();
        let roots = vec![root.canonicalize().unwrap()];

        // Textually this starts with the root; canonicalised it does not.
        let sneaky = root.join("..").join("outside");
        let resolved = resolve_path(sneaky.to_str().unwrap()).unwrap();
        assert!(
            !is_within_roots(&resolved, &roots),
            "`..` traversal escaped the allowlist: {}",
            resolved.display()
        );
    }

    #[cfg(unix)]
    #[test]
    fn symlink_out_of_a_root_cannot_escape() {
        let base = tmp("symlink");
        let root = base.join("root");
        let outside = base.join("outside");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::create_dir_all(&outside).unwrap();
        std::os::unix::fs::symlink(&outside, root.join("escape")).unwrap();
        let roots = vec![root.canonicalize().unwrap()];

        let resolved = resolve_path(root.join("escape").to_str().unwrap()).unwrap();
        assert!(
            !is_within_roots(&resolved, &roots),
            "symlink escaped the allowlist: {}",
            resolved.display()
        );
    }

    #[test]
    fn sibling_with_a_shared_name_prefix_is_not_inside() {
        // "/a/root-evil" must not count as inside "/a/root" just because the string starts with it.
        let base = tmp("prefix");
        let root = base.join("root");
        let evil = base.join("root-evil");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::create_dir_all(&evil).unwrap();
        let roots = vec![root.canonicalize().unwrap()];
        let resolved = resolve_path(evil.to_str().unwrap()).unwrap();
        assert!(!is_within_roots(&resolved, &roots));
    }
}
