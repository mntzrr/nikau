//! Self-update: pull the latest monux source, rebuild, and install.
//!
//! The source is cloned once into a cache dir (~/.cache/monux/src) and pulled
//! on each update. Building from source on this machine matters: the repo's
//! .cargo/config.toml sets target-cpu=native, so a binary built elsewhere can
//! crash with an illegal instruction on a CPU with fewer features.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use tracing::{debug, info};

const DEFAULT_REPO: &str = "https://github.com/mntzrr/monux.git";
/// Commit this binary was built from, set by build.rs ("<sha>" or "<sha>-dirty").
pub const CURRENT_REVISION: &str = env!("MONUX_GIT_SHA");

/// The repo updates are pulled from (MONUX_UPDATE_REPO overrides for testing).
pub fn repo_url() -> String {
    std::env::var("MONUX_UPDATE_REPO").unwrap_or_else(|_| DEFAULT_REPO.to_string())
}

/// The commit currently published at the repo's HEAD (cheap update check; no
/// clone needed).
pub fn latest_remote_sha(repo: &str) -> Result<String> {
    let out = git_network_command()
        .args(["ls-remote", repo, "HEAD"])
        .output()
        .context("Failed to run git: is it installed?")?;
    if !out.status.success() {
        bail!("git ls-remote {} failed", repo);
    }
    let stdout = String::from_utf8(out.stdout)?;
    stdout
        .split_whitespace()
        .next()
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .with_context(|| format!("git ls-remote {} returned no HEAD", repo))
}

/// Whether the remote HEAD sha means there's an update for a build with the
/// given revision ("<sha>" or "<sha>-dirty"; "unknown" never auto-updates).
pub fn is_newer_remote(remote_sha: &str, current_revision: &str) -> bool {
    let current_base = current_revision.trim_end_matches("-dirty");
    current_base != "unknown" && !current_base.is_empty() && !remote_sha.starts_with(current_base)
}

/// How an update attempt ended.
pub enum UpdateStatus {
    /// A new build was installed.
    Installed,
    /// Already up to date; nothing was built.
    AlreadyCurrent,
    /// The new source speaks a different protocol version than our server;
    /// nothing was built (see the protocol_constraint parameter of run).
    SkippedIncompatible,
}

pub fn run(force: bool, low_priority: bool, protocol_constraint: Option<u64>) -> Result<UpdateStatus> {
    let repo = repo_url();
    let src_dir = match std::env::var_os("MONUX_UPDATE_CACHE") {
        Some(dir) => PathBuf::from(dir),
        None => home::home_dir()
            .context("No home dir found")?
            .join(".cache")
            .join("monux")
            .join("src"),
    };

    if src_dir.join(".git").exists() {
        info!("Pulling latest source in {}...", src_dir.display());
        git(&src_dir, &["pull", "--ff-only"]).with_context(|| {
            format!(
                "Failed to update the source checkout; delete it and retry: rm -rf {}",
                src_dir.display()
            )
        })?;
    } else {
        info!("Cloning {} into {}...", repo, src_dir.display());
        if let Some(parent) = src_dir.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("Failed to create {}", parent.display()))?;
        }
        let status = git_network_command()
            .args(["clone", "--depth", "1", &repo])
            .arg(&src_dir)
            .status()
            .context("Failed to run git: is it installed?")?;
        if !status.success() {
            bail!("git clone {} failed", repo);
        }
    }

    let latest = git_output(&src_dir, &["rev-parse", "--short=12", "HEAD"])?;
    let current_base = CURRENT_REVISION.trim_end_matches("-dirty");
    if !force && current_base != "unknown" && latest == current_base {
        info!(
            "monux is already up to date ({}). Use --force to rebuild anyway.",
            CURRENT_REVISION
        );
        return Ok(UpdateStatus::AlreadyCurrent);
    }

    // The update gate: a client never installs a build whose protocol version
    // differs from its server's — it would be unable to reconnect. Checked
    // from the pulled source, before the expensive build.
    if !force {
        if let Some(server_version) = protocol_constraint {
            let source_version = source_protocol_version(&src_dir)?;
            if source_version != server_version {
                info!(
                    "Not updating to {}: it speaks protocol v{}, but the server speaks v{}. Update the server first; this gate opens automatically once this client reconnects to it (or use --force to override).",
                    latest, source_version, server_version
                );
                return Ok(UpdateStatus::SkippedIncompatible);
            }
        }
    }
    info!("Updating monux: {} -> {}", CURRENT_REVISION, latest);

    let root = install_root();
    let cargo = find_cargo()?;
    // Clean staging leftovers from previously killed installs. Skip dirs whose
    // pid suffix is a live process: a concurrent updater is building there.
    let our_pid = std::process::id() as i32;
    if let Ok(entries) = std::fs::read_dir(&root) {
        for entry in entries.flatten() {
            if let Some(pid_str) = entry
                .file_name()
                .to_string_lossy()
                .strip_prefix(".monux-install-staging-")
            {
                if let Ok(pid) = pid_str.parse::<i32>() {
                    if pid != our_pid && std::path::Path::new(&format!("/proc/{}", pid)).exists() {
                        continue;
                    }
                }
                let _ = std::fs::remove_dir_all(entry.path());
            }
        }
    }
    // Install into a staging dir on the same filesystem, then rename the
    // binary into place atomically. 'cargo install' copies into bin/ in
    // place, so a kill mid-copy could leave a truncated monux binary;
    // rename(2) of a complete file replaces atomically instead.
    let staging = root.join(format!(".monux-install-staging-{}", std::process::id()));
    info!(
        "Building and installing to {} (this can take a few minutes)...",
        root.join("bin/monux").display()
    );
    let mut cmd = if low_priority {
        // Background auto-updates compile at the lowest CPU scheduling
        // priority, so a build can't stall interactive input on this machine.
        let mut c = Command::new("nice");
        c.args(["-n", "19"]).arg(cargo);
        c
    } else {
        Command::new(cargo)
    };
    let status = cmd
        .arg("install")
        // Build exactly the locked dependencies (Cargo.lock is committed).
        .arg("--locked")
        .arg("--path")
        .arg(&src_dir)
        .arg("--root")
        .arg(&staging)
        .arg("--force")
        // cargo warns when the install root's bin/ isn't on PATH. The staging
        // root is transient (the binary is renamed out of it below), so put it
        // on PATH just for this subprocess to silence the misleading warning.
        .env("PATH", path_with(staging.join("bin")))
        .status()
        .context("Failed to run cargo install")?;
    if !status.success() {
        let _ = std::fs::remove_dir_all(&staging);
        bail!("cargo install failed");
    }
    place_binary_atomically(
        &staging.join("bin").join("monux"),
        &root.join("bin").join("monux"),
    )?;
    let _ = std::fs::remove_dir_all(&staging);
    info!(
        "Updated monux to {} at {}. Restart any running monux server/client to pick it up.",
        latest,
        root.join("bin/monux").display()
    );
    Ok(UpdateStatus::Installed)
}

/// Reads the protocol version a source checkout speaks, straight from its
/// shared.rs — no build needed.
fn source_protocol_version(src_dir: &Path) -> Result<u64> {
    let shared_rs = src_dir.join("src").join("msgs").join("shared.rs");
    let text = std::fs::read_to_string(&shared_rs)
        .with_context(|| format!("Failed to read {}", shared_rs.display()))?;
    for line in text.lines() {
        let line = line.trim();
        if let Some(rest) = line.strip_prefix("pub const PROTOCOL_VERSION: u64 =") {
            if let Some(num) = rest.trim().strip_suffix(';') {
                return num.trim().parse().with_context(|| {
                    format!("Failed to parse PROTOCOL_VERSION in {}", shared_rs.display())
                });
            }
        }
    }
    bail!("PROTOCOL_VERSION not found in {}", shared_rs.display())
}

/// Name of the file (inside the config dir) recording the protocol version of
/// the server this machine last talked to as a client.
const SERVER_PROTOCOL_VERSION_FILE: &str = "server_protocol_version";

/// How long a recorded server protocol version stays authoritative. Every
/// handshake rewrites the file, so an older record means this machine has not
/// acted as a client in days — typically a pure server, whose record can never
/// heal otherwise (its own mDNS advertisement is ignored for the gate and it
/// never handshakes as a client). Treating an expired record as absent lets
/// such a machine update again instead of being vetoed by history forever.
const SERVER_PROTOCOL_VERSION_MAX_AGE: Duration = Duration::from_secs(48 * 60 * 60);

/// Records the server's protocol version for the update gate (best-effort).
/// Called by the client on every handshake, including refused ones — that is
/// what re-opens the gate after the server upgrades ahead of us.
pub fn record_server_protocol_version(config_dir: &Path, version: u64) {
    if let Err(e) = std::fs::write(
        config_dir.join(SERVER_PROTOCOL_VERSION_FILE),
        version.to_string(),
    ) {
        tracing::warn!("Failed to record server protocol version: {:?}", e);
    }
}

/// The protocol version of the server this machine acts as a client to, if it
/// has connected to one recently. Used to gate updates so a client never
/// installs a build its server couldn't talk to. Records older than
/// SERVER_PROTOCOL_VERSION_MAX_AGE are ignored (see there).
pub fn server_protocol_constraint(config_dir: &Path) -> Option<u64> {
    server_protocol_constraint_fresh(config_dir, SERVER_PROTOCOL_VERSION_MAX_AGE)
}

/// The max age is a parameter so tests can force expiry without touching
/// file mtimes.
fn server_protocol_constraint_fresh(config_dir: &Path, max_age: Duration) -> Option<u64> {
    let path = config_dir.join(SERVER_PROTOCOL_VERSION_FILE);
    let version: u64 = std::fs::read_to_string(&path).ok()?.trim().parse().ok()?;
    let age = match std::fs::metadata(&path).and_then(|m| m.modified()) {
        // A future mtime (clock skew) counts as fresh.
        Ok(mtime) => mtime.elapsed().unwrap_or(Duration::ZERO),
        Err(_) => return None,
    };
    (age <= max_age).then_some(version)
}

/// Deletes the recorded server protocol version (the update gate file).
/// Called at server startup when no client runs on this machine: on a pure
/// server the record is stale history that cannot heal by itself — nothing
/// ever rewrites it — and it vetoes manual updates while the daemon happens
/// to be down (mDNS finds no live server to refresh it then).
pub fn clear_protocol_constraint(config_dir: &Path) {
    let path = config_dir.join(SERVER_PROTOCOL_VERSION_FILE);
    match std::fs::remove_file(&path) {
        Ok(()) => info!(
            "Cleared the recorded server protocol version: this machine runs only a server, so the client-side update gate does not apply"
        ),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => tracing::warn!("Failed to clear the server protocol version gate file: {:?}", e),
    }
}

/// The server protocol version to gate an update on: the minimum of the
/// versions Monux servers currently advertise via mDNS (also recorded, healing
/// a stale gate file), falling back to the version this client recorded at its
/// last handshake when no server answers (offline, another subnet, or a build
/// predating the advertisement). Never fails: discovery is best-effort. Blocks
/// for up to the mDNS discovery timeout: call it from a blocking context.
pub fn refresh_protocol_constraint(config_dir: Option<&Path>) -> Option<u64> {
    let recorded = config_dir.and_then(server_protocol_constraint);
    let discovered = match crate::discovery::discover_server_protocol_versions() {
        Ok(versions) => versions,
        Err(e) => {
            debug!(
                "Server protocol version discovery failed ({}); using the recorded gate value",
                e
            );
            return recorded;
        }
    };
    let constraint = match crate::discovery::protocol_version_constraint(&discovered) {
        Some(constraint) => constraint,
        // No server answered: fall back to the last recorded version.
        None => return recorded,
    };
    if discovered.len() > 1 {
        info!(
            "Monux servers advertise different protocol versions ({}); gating on the oldest, v{}",
            discovered
                .iter()
                .map(u64::to_string)
                .collect::<Vec<_>>()
                .join(", "),
            constraint
        );
    }
    if recorded != Some(constraint) {
        info!(
            "Refreshed the server protocol version gate via mDNS: v{} -> v{}",
            recorded
                .map(|v| v.to_string())
                .unwrap_or_else(|| "<none>".to_string()),
            constraint
        );
    }
    if let Some(dir) = config_dir {
        let _ = std::fs::create_dir_all(dir);
        record_server_protocol_version(dir, constraint);
    }
    Some(constraint)
}

/// Install next to the currently running binary (<root>/bin/monux -> <root>),
/// falling back to ~/.local.
fn install_root() -> PathBuf {
    if let Ok(exe) = std::env::current_exe() {
        // After an auto-update replaces the binary on disk while we run, Linux
        // reports our exe as "<path> (deleted)"; trim that suffix.
        let exe = PathBuf::from(exe.to_string_lossy().trim_end_matches(" (deleted)"));
        if exe.file_name().is_some_and(|name| name == "monux") {
            if let Some(bin_dir) = exe.parent() {
                if bin_dir.file_name().is_some_and(|name| name == "bin") {
                    if let Some(root) = bin_dir.parent() {
                        return root.to_path_buf();
                    }
                }
            }
        }
    }
    home::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".local")
}

/// Moves the staged binary onto its final path via rename(2), which replaces
/// atomically on the same filesystem: a kill at any point leaves either the
/// old or the new binary intact, never a partial one. The staging dir lives
/// inside the install root, so the two paths are always on the same
/// filesystem (renames across filesystems would fail rather than copy).
fn place_binary_atomically(from: &Path, to: &Path) -> Result<()> {
    if let Some(parent) = to.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create {}", parent.display()))?;
    }
    std::fs::rename(from, to).with_context(|| {
        format!(
            "Failed to move {} into place at {}",
            from.display(),
            to.display()
        )
    })
}

/// PATH with `dir` prepended, for a subprocess (see the cargo install call).
fn path_with(dir: PathBuf) -> std::ffi::OsString {
    let mut paths = vec![dir];
    if let Some(path) = std::env::var_os("PATH") {
        paths.extend(std::env::split_paths(&path));
    }
    std::env::join_paths(paths).expect("PATH entries can't contain NUL")
}

/// cargo from PATH if runnable, else the rustup default location (PATH can be
/// minimal depending on how monux was launched).
fn find_cargo() -> Result<PathBuf> {
    let in_path = Command::new("cargo")
        .arg("--version")
        .output()
        .map(|out| out.status.success())
        .unwrap_or(false);
    if in_path {
        return Ok(PathBuf::from("cargo"));
    }
    let fallback = home::home_dir()
        .context("No home dir found")?
        .join(".cargo")
        .join("bin")
        .join("cargo");
    if fallback.exists() {
        return Ok(fallback);
    }
    bail!("cargo not found: install a Rust toolchain via https://rustup.rs/")
}

fn git(dir: &Path, args: &[&str]) -> Result<()> {
    let status = git_network_command()
        .arg("-C")
        .arg(dir)
        .args(args)
        .status()
        .context("Failed to run git: is it installed?")?;
    if !status.success() {
        bail!("git {:?} failed in {}", args, dir.display());
    }
    Ok(())
}

/// A git command for network operations (ls-remote/clone/pull), bounded so a
/// dead route or hung connection fails in ~30s instead of blocking for
/// minutes: git aborts when the transfer rate stays below
/// GIT_HTTP_LOW_SPEED_LIMIT bytes/sec for GIT_HTTP_LOW_SPEED_TIME seconds.
fn git_network_command() -> Command {
    let mut cmd = Command::new("git");
    cmd.env("GIT_HTTP_LOW_SPEED_LIMIT", "1000")
        .env("GIT_HTTP_LOW_SPEED_TIME", "30");
    cmd
}

fn git_output(dir: &Path, args: &[&str]) -> Result<String> {
    let out = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .output()
        .context("Failed to run git: is it installed?")?;
    if !out.status.success() {
        bail!("git {:?} failed in {}", args, dir.display());
    }
    Ok(String::from_utf8(out.stdout)?.trim().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn update_check_comparison() {
        // Different commit: update available.
        assert!(is_newer_remote(
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "bbbbbbbbbbbb"
        ));
        // Remote HEAD is our commit (possibly with more context): up to date.
        assert!(!is_newer_remote(
            "bbbbbbbbbbbbcccccccccccccccccccccccc",
            "bbbbbbbbbbbb"
        ));
        // Dirty build compares against its base sha.
        assert!(!is_newer_remote(
            "bbbbbbbbbbbbcccccccccccccccccccccccc",
            "bbbbbbbbbbbb-dirty"
        ));
        // Unknown build revision: never auto-update.
        assert!(!is_newer_remote(
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "unknown"
        ));
    }

    #[test]
    fn parses_own_source_protocol_version() {
        // Guards the gate against repo layout drift: the parser must find the
        // constant this very binary was built with.
        let own = Path::new(env!("CARGO_MANIFEST_DIR"));
        assert_eq!(
            source_protocol_version(own).unwrap(),
            crate::msgs::shared::PROTOCOL_VERSION
        );
    }

    #[test]
    fn server_protocol_constraint_roundtrip() {
        let dir =
            std::env::temp_dir().join(format!("monux-test-constraint-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        // Never connected: no constraint.
        assert_eq!(server_protocol_constraint(&dir), None);
        record_server_protocol_version(&dir, 7);
        assert_eq!(server_protocol_constraint(&dir), Some(7));
        // A later handshake overwrites (e.g. the server upgraded).
        record_server_protocol_version(&dir, 8);
        assert_eq!(server_protocol_constraint(&dir), Some(8));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn server_protocol_constraint_expires() {
        let dir =
            std::env::temp_dir().join(format!("monux-test-constraint-exp-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        record_server_protocol_version(&dir, 9);
        // Fresh record: honored.
        assert_eq!(
            server_protocol_constraint_fresh(&dir, Duration::from_secs(60)),
            Some(9)
        );
        // A record not refreshed within the max age is ignored: this is what
        // lets a pure server (nothing rewrites its file) update again.
        assert_eq!(server_protocol_constraint_fresh(&dir, Duration::ZERO), None);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn clear_protocol_constraint_removes_the_file() {
        let dir = std::env::temp_dir().join(format!(
            "monux-test-constraint-clear-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        record_server_protocol_version(&dir, 9);
        assert_eq!(server_protocol_constraint(&dir), Some(9));
        clear_protocol_constraint(&dir);
        assert_eq!(server_protocol_constraint(&dir), None);
        // Idempotent: a missing file is not an error.
        clear_protocol_constraint(&dir);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn path_with_prepends_the_dir_and_preserves_path() {
        let path = path_with(PathBuf::from("/tmp/monux-staging-bin"));
        let paths: Vec<PathBuf> = std::env::split_paths(&path).collect();
        assert_eq!(paths[0], PathBuf::from("/tmp/monux-staging-bin"));
        let original: Vec<PathBuf> =
            std::env::split_paths(&std::env::var_os("PATH").unwrap_or_default()).collect();
        assert_eq!(&paths[1..], original.as_slice());
    }

    #[test]
    fn place_binary_atomically_replaces_the_target() {
        let dir =
            std::env::temp_dir().join(format!("monux-test-atomic-place-{}", std::process::id()));
        let staging = dir.join("staging");
        let bin = dir.join("root").join("bin");
        std::fs::create_dir_all(&staging).unwrap();
        std::fs::create_dir_all(&bin).unwrap();
        let from = staging.join("monux");
        let to = bin.join("monux");
        std::fs::write(&from, b"new-binary").unwrap();
        std::fs::write(&to, b"old-binary").unwrap();
        place_binary_atomically(&from, &to).unwrap();
        // The target is replaced wholesale and the staged file is consumed.
        assert_eq!(std::fs::read(&to).unwrap(), b"new-binary");
        assert!(!from.exists());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
