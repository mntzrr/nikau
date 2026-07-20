//! Ensures only one server (and one client) instance runs at a time.
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
//! nikau (HOME=/root) and a user-run nikau (HOME=/home/user) must never run
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
const LOCK_DIR_ENV: &str = "NIKAU_LOCK_DIR";

/// Holds the single-instance flock for the lifetime of the process.
pub struct InstanceLock {
    _file: fs::File,
}

/// Filesystem path of the lock file for `kind` ("server" or "client").
fn lock_path(kind: &str) -> PathBuf {
    let dir = std::env::var_os(LOCK_DIR_ENV)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/tmp"));
    dir.join(format!("nikau-{}.lock", kind))
}

/// Takes the single-instance lock for `kind` ("server" or "client").
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
        return Ok(InstanceLock { _file: file });
    }

    // Another instance holds the lock and is alive (flocks can't go stale).
    let pid = read_pid(&path).with_context(|| {
        format!(
            "Another nikau {} is already running, but its pid couldn't be determined (lock: {}). Stop it manually.",
            kind,
            path.display()
        )
    })?;
    if pid == std::process::id() as i32 {
        // Defensive: we can't be the holder and fail to lock our own file.
        bail!("Another nikau {} is already running (lock: {})", kind, path.display());
    }
    // Guard against pid reuse or a stale/poisoned pid file: only signal a
    // process whose executable is actually nikau running the matching kind.
    // comm (exact exe name) rules out wrapper shells whose cmdline merely
    // contains the nikau invocation.
    let comm = fs::read_to_string(format!("/proc/{}/comm", pid)).unwrap_or_default();
    let cmdline = fs::read_to_string(format!("/proc/{}/cmdline", pid))
        .map(|s| s.replace('\0', " "))
        .unwrap_or_default();
    if comm.trim() != "nikau" || !cmdline.contains(kind) {
        bail!(
            "Another nikau {0} is already running, but the pid recorded in {1} ({2}) doesn't look like a nikau {0} process (comm: '{3}'). Refusing to kill it; stop the old instance manually.",
            kind, path.display(), pid, comm.trim()
        );
    }
    info!("Asking existing nikau {} (pid {}) to shut down...", kind, pid);
    if unsafe { libc::kill(pid, libc::SIGTERM) } != 0 {
        bail!(
            "Failed to SIGTERM existing nikau {} (pid {}): {}. Stop it manually.",
            kind,
            pid,
            std::io::Error::last_os_error()
        );
    }

    let deadline = Instant::now() + TAKEOVER_TIMEOUT;
    loop {
        if try_lock(&file) {
            info!("Previous nikau {} exited, taking over", kind);
            write_pid(&file);
            return Ok(InstanceLock { _file: file });
        }
        if Instant::now() >= deadline {
            bail!(
                "Existing nikau {} (pid {}) did not exit within {}s of SIGTERM. Kill it manually (kill -9 {}) and retry.",
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
