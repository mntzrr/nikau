//! Ensures only one server (and one client, and one tray indicator) instance
//! runs at a time.
//!
//! The guarantee is an flock on a file in a machine-wide directory (/tmp by
//! default). Because flock is tied to the open file description, the kernel
//! releases it when the process dies for any reason (crash, kill -9, panic),
//! so the lock can never go stale; the file itself is deliberately never
//! deleted. When a new instance finds the lock held, the holder is therefore
//! definitely alive: it is asked to shut down (SIGTERM) and the new instance
//! takes over.
//!
//! The lock deliberately does NOT live in the per-user config dir: a root-run
//! monux (HOME=/root) and a user-run monux (HOME=/home/user) must never run
//! side by side, since two instances on one machine fight over keyboard grabs
//! and virtual devices (seen in the wild as endless grab-retry log spam).
//! The file is created world-writable (0666) so that instances running as
//! different users can still see and lock it; flock itself is the authority,
//! so a scribbled pid file can at worst cause a confusing refusal, never a
//! wrongful kill (the pid is verified against /proc before signaling).
//! One caveat: long-lived /tmp cleaners (e.g. systemd-tmpfiles) may delete
//! the file from under a very long-lived holder, weakening the guarantee.

use std::fs;
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use tracing::info;

/// How long to wait for a previous instance to exit after SIGTERM.
const TAKEOVER_TIMEOUT: Duration = Duration::from_secs(5);

/// Environment variable overriding the lock directory; used by tests to
/// avoid colliding with real instances on the same machine.
const LOCK_DIR_ENV: &str = "MONUX_LOCK_DIR";

/// Holds the single-instance flock for the lifetime of the process.
pub struct InstanceLock {
    _file: fs::File,
    /// True when this instance took over from a previous live instance (we
    /// SIGTERM'd it), rather than starting on a free lock. Callers that
    /// create devices (uinput) should let the previous instance's device
    /// teardown settle before creating their own.
    pub took_over: bool,
}

/// Filesystem path of the lock file for `kind` ("server", "client" or
/// "indicator").
fn lock_path(kind: &str) -> PathBuf {
    let dir = std::env::var_os(LOCK_DIR_ENV)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/tmp"));
    dir.join(format!("monux-{}.lock", kind))
}

/// Takes the single-instance lock for `kind` ("server", "client" or
/// "indicator").
/// If a previous instance holds it, that instance is asked to shut down
/// (SIGTERM) and this call blocks until it exits (up to a few seconds),
/// then takes over. The lock file records the holder's pid as a human hint
/// and to find the process to signal; the flock itself is authoritative.
pub fn acquire(kind: &str) -> Result<InstanceLock> {
    let path = lock_path(kind);
    // 0666 so that instances running as other users (e.g. root via sudo)
    // share the same lock; chmod too since umask may restrict the create mode.
    let file = match fs::OpenOptions::new()
        .create(true)
        .write(true)
        .mode(0o666)
        .open(&path)
    {
        Ok(file) => file,
        // The file exists and is owned by another user with stricter perms
        // (e.g. created before the 0666 scheme): flock works on a read-only fd.
        Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => fs::OpenOptions::new()
            .read(true)
            .open(&path)
            .with_context(|| format!("Failed to open {} lock file: {}", kind, path.display()))?,
        Err(e) => {
            return Err(e)
                .with_context(|| format!("Failed to open {} lock file: {}", kind, path.display()))
        }
    };
    let _ = fs::set_permissions(&path, std::os::unix::fs::PermissionsExt::from_mode(0o666));
    if try_lock(&file) {
        write_pid(&file);
        return Ok(InstanceLock {
            _file: file,
            took_over: false,
        });
    }

    // Another instance holds the lock and is alive (flocks can't go stale).
    let pid = read_pid(&path).with_context(|| {
        format!(
            "Another monux {} is already running, but its pid couldn't be determined (lock: {}). Stop it manually.",
            kind,
            path.display()
        )
    })?;
    if pid == std::process::id() as i32 {
        // Defensive: we can't be the holder and fail to lock our own file.
        bail!("Another monux {} is already running (lock: {})", kind, path.display());
    }
    // Guard against pid reuse or a stale/poisoned pid file: only signal a
    // process whose executable is actually monux running the matching kind.
    // comm (exact exe name) rules out wrapper shells whose cmdline merely
    // contains the monux invocation.
    let comm = fs::read_to_string(format!("/proc/{}/comm", pid));
    let cmdline =
        fs::read_to_string(format!("/proc/{}/cmdline", pid)).map(|s| s.replace('\0', " "));
    let verified = matches!(
        (&comm, &cmdline),
        (Ok(c), Ok(cl)) if c.trim() == "monux" && cl.split_whitespace().any(|tok| tok == kind)
    );
    if !verified {
        if comm.is_err() && cmdline.is_err() {
            // /proc/<pid> is unreadable for another user's process (hidepid):
            // the holder is likely root, and we can neither verify nor signal it.
            bail!(
                "Another monux {0} (pid {1}) is already running as a different user (likely root), so it can't be inspected or signaled from here. Stop it manually: sudo kill {1}",
                kind, pid
            );
        }
        bail!(
            "Another monux {0} is already running, but the pid recorded in {1} ({2}) doesn't look like a monux {0} process (comm: '{3}'). Refusing to kill it; stop the old instance manually.",
            kind,
            path.display(),
            pid,
            comm.map(|c| c.trim().to_string()).unwrap_or_default()
        );
    }
    info!("Asking existing monux {} (pid {}) to shut down...", kind, pid);
    // We re-exec'd after an auto-update (MONUX_RESTARTED). exec() released
    // our flock (the fd is CLOEXEC), and a contender (autostart, systemd,
    // manual restart) may have acquired the free lock during the startup
    // gap before we reached acquire(). Yield instead of killing them — the
    // updated binary is already on disk for their next restart, and a
    // kill-and-takeover here ping-pongs indefinitely.
    if std::env::var_os("MONUX_RESTARTED").is_some() {
        info!(
            "Update restart found another monux {} already running; yielding to avoid a takeover ping-pong",
            kind
        );
        bail!("Another monux {} is already running", kind);
    }
    if unsafe { libc::kill(pid, libc::SIGTERM) } != 0 {
        let err = std::io::Error::last_os_error();
        if err.raw_os_error() == Some(libc::EPERM) {
            bail!(
                "Another monux {0} (pid {1}) is already running as a different user (likely root). Stop it manually: sudo kill {1}",
                kind, pid
            );
        }
        bail!(
            "Failed to SIGTERM existing monux {} (pid {}): {}. Stop it manually.",
            kind,
            pid,
            err
        );
    }

    let deadline = Instant::now() + TAKEOVER_TIMEOUT;
    loop {
        if try_lock(&file) {
            info!("Previous monux {} exited, taking over", kind);
            write_pid(&file);
            return Ok(InstanceLock {
                _file: file,
                took_over: true,
            });
        }
        if Instant::now() >= deadline {
            bail!(
                "Existing monux {} (pid {}) did not exit within {}s of SIGTERM. Kill it manually (kill -9 {}) and retry.",
                kind,
                pid,
                TAKEOVER_TIMEOUT.as_secs(),
                pid
            );
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn try_lock(file: &fs::File) -> bool {
    rustix::fs::flock(file, rustix::fs::FlockOperation::NonBlockingLockExclusive).is_ok()
}

/// Writes our pid into the lock file (human hint + takeover lookup).
/// Best-effort: the fd may be read-only when the file is owned by another user.
fn write_pid(file: &fs::File) {
    let _ = file.set_len(0);
    let mut w = file;
    let _ = writeln!(w, "pid {}", std::process::id());
}

/// Reads the holder's pid from the lock file, as written by write_pid.
fn read_pid(path: &PathBuf) -> Option<i32> {
    fs::read_to_string(path).ok()?.trim().strip_prefix("pid ")?.parse().ok()
}

/// The pid of the live holder of the `kind` lock, if there is one: the lock
/// file records a pid and that process is alive and looks like monux running
/// the matching kind. Read-only role detection (e.g. the update gate decides
/// whether this machine is a pure server) — it never disturbs the lock.
///
/// The pid file is a human hint, not authority (flocks can't go stale, but
/// the pid file can point at a reused pid). To reject a stale entry we probe
/// the flock: if we can acquire it, the lock is free and the pid file is
/// stale. We immediately release — this is a read-only check.
pub fn live_holder(kind: &str) -> Option<i32> {
    let path = lock_path(kind);
    // Probe the flock: if it's free, the pid file is stale (the old holder
    // crashed without flock-release being an issue — flock auto-releases on
    // process death — but the pid file survived). flock works on a read-only
    // fd, so this handles the cross-user case too.
    let probe = match fs::OpenOptions::new().read(true).open(&path) {
        Ok(f) => {
            if try_lock(&f) {
                let _ = rustix::fs::flock(&f, rustix::fs::FlockOperation::Unlock);
                return None;
            }
            true // locked by someone else
        }
        Err(_) => return None,
    };
    debug_assert!(probe);
    let pid = read_pid(&path)?;
    if pid == std::process::id() as i32 {
        return None;
    }
    let comm = fs::read_to_string(format!("/proc/{}/comm", pid)).ok()?;
    let cmdline = fs::read_to_string(format!("/proc/{}/cmdline", pid))
        .map(|s| s.replace('\0', " "))
        .ok()?;
    // Exact argv-token match: `monux client my-server-host` must NOT match
    // live_holder("server") — a bare substring check would.
    if comm.trim() == "monux" && cmdline.split_whitespace().any(|tok| tok == kind) {
        Some(pid)
    } else {
        None
    }
}
