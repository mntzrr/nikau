//! Ensures only one server (and one client) instance runs at a time.
//!
//! The guarantee is an flock on a file in the config dir. Because flock is
//! tied to the open file description, the kernel releases it when the process
//! dies for any reason (crash, kill -9, panic), so the lock can never go
//! stale; the file itself is deliberately never deleted. When a new instance
//! finds the lock held, the holder is therefore definitely alive: it is
//! asked to shut down (SIGTERM) and the new instance takes over.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use tracing::info;

/// How long to wait for a previous instance to exit after SIGTERM.
const TAKEOVER_TIMEOUT: Duration = Duration::from_secs(5);

/// Holds the single-instance flock for the lifetime of the process.
pub struct InstanceLock {
    _file: fs::File,
}

/// Takes the single-instance lock for `kind` ("server" or "client").
/// If a previous instance holds it, that instance is asked to shut down
/// (SIGTERM) and this call blocks until it exits (up to a few seconds),
/// then takes over. The lock file records the holder's pid as a human hint
/// and to find the process to signal; the flock itself is authoritative.
pub fn acquire(config_dir: &Path, kind: &str) -> Result<InstanceLock> {
    let path = config_dir.join(format!("{}.lock", kind));
    let file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(&path)
        .with_context(|| format!("Failed to open {} lock file: {}", kind, path.display()))?;
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
fn write_pid(file: &fs::File) {
    let _ = file.set_len(0);
    let mut w = file;
    let _ = writeln!(w, "pid {}", std::process::id());
}

/// Reads the holder's pid from the lock file, as written by write_pid.
fn read_pid(path: &PathBuf) -> Option<i32> {
    fs::read_to_string(path).ok()?.trim().strip_prefix("pid ")?.parse().ok()
}
