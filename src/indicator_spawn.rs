//! Auto-spawning the tray indicator (`monux system indicator`) alongside
//! `monux server` / `monux client`, so the tray icon appears whenever a
//! daemon runs in a desktop session — no separate compositor autostart entry
//! needed.
//!
//! The daemon spawns the indicator as a child process (its own binary with
//! `system indicator`), inheriting stdout/stderr so the indicator's log lines
//! fold into the daemon's log. The child is supervised:
//!
//! - Graceful daemon shutdown (any path: SIGTERM/SIGINT, control-socket
//!   exit/restart, --exit-secs, a failed daemon loop) drops the Supervisor,
//!   which SIGTERMs the child and reaps it. The auto-update re-exec is the
//!   same path: the old image shuts down (child dies), the new image spawns
//!   a fresh indicator.
//! - A daemon SIGKILL skips all of this: the orphaned indicator keeps
//!   showing its "?" state until the NEXT daemon's indicator takes over via
//!   the single-instance lock (single_instance.rs, kind "indicator"), which
//!   also keeps a manually-started indicator from duplicating the icon.
//! - If the indicator exits on its own (its tray host, e.g. waybar, died),
//!   the supervisor respawns it, bounded by RespawnPolicy; after giving up it
//!   stays down until `monux system tray show` (or a manual indicator).
//!
//! The icon can be HIDDEN without killing the daemon and SHOWN again without
//! restarting it (control socket `{"cmd":"indicator","action":...}`, driven
//! by the tray menu's "Hide tray icon" and by `monux system tray hide|show`;
//! see SupervisorHandle). Hidden means: the spawned child is SIGTERM'd and
//! the supervisor neither spawns nor respawns (the respawn policy stays
//! dormant). The hidden state is IN-MEMORY ONLY: a daemon (re)start always
//! spawns the indicator fresh. The supervisor only ever manages ITS spawned
//! child — a manually-started indicator is never killed or respawn-guarded
//! by it (a SIGTERM death, e.g. a takeover, just parks the supervisor in the
//! hidden state until show()).
//!
//! Spawning is skipped (debug log, never an error) when the user opted out
//! (--no-indicator / MONUX_NO_INDICATOR) or when there is no desktop session
//! bus to talk to — the indicator would only fail noisily there. The opt-out
//! is explicit and sticky: a control-socket show does NOT override it.

use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use tracing::{debug, info, warn};

/// Maximum respawns before the supervisor gives up (see RespawnPolicy).
const MAX_RESPAWNS: u32 = 3;
/// Delay between an unexpected indicator exit and its respawn.
const RESPAWN_DELAY: Duration = Duration::from_secs(30);
/// An indicator that stayed up this long counts as stable: the respawn
/// counter resets, so a rare-but-recurring tray-host death never exhausts it.
const STABLE_UPTIME: Duration = Duration::from_secs(10 * 60);
/// Shutdown/hide: how long to wait for the SIGTERM'd child before escalating
/// to SIGKILL. The indicator dies on SIGTERM immediately (default disposition).
const TERM_GRACE: Duration = Duration::from_secs(2);
/// How often the monitor task polls the child's liveness. Coarse on purpose:
/// try_wait is cheap and a sub-second detection delay is irrelevant here.
const POLL_INTERVAL: Duration = Duration::from_millis(500);

/// Why the supervisor decided not to spawn (drives the debug log; tested).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SpawnVeto {
    /// --no-indicator or MONUX_NO_INDICATOR.
    OptedOut,
    /// No desktop session bus: the indicator would fail noisily.
    NoDesktopSession,
}

/// Decides whether to auto-spawn the indicator, from pre-probed facts (the
/// probes stay out of this function so the logic is testable without touching
/// the process environment or the filesystem).
pub fn spawn_veto(
    no_indicator_flag: bool,
    no_indicator_env: bool,
    session_bus_env: bool,
    user_bus_socket: bool,
) -> Option<SpawnVeto> {
    if no_indicator_flag || no_indicator_env {
        return Some(SpawnVeto::OptedOut);
    }
    if !session_bus_env && !user_bus_socket {
        return Some(SpawnVeto::NoDesktopSession);
    }
    None
}

/// Probes for a desktop session bus the indicator could talk to: the bus
/// address is set, or the per-user bus socket exists (the systemd/dbus-broker
/// layout, where the address is derivable from the uid even when the env var
/// isn't set). Used by the auto-spawn guard, by show() (re-probed live: the
/// session may have appeared since the daemon started), and by the manual
/// `monux system indicator` entry point, which checks this before touching
/// the single-instance lock so headless sessions exit cleanly, lock-free.
pub fn has_desktop_session() -> bool {
    probe_session_bus_env() || probe_user_bus_socket()
}

fn probe_session_bus_env() -> bool {
    std::env::var_os("DBUS_SESSION_BUS_ADDRESS").is_some()
}

fn probe_user_bus_socket() -> bool {
    Path::new(&format!("/run/user/{}/bus", unsafe { libc::geteuid() })).exists()
}

/// The bounded-respawn decision, factored out of the supervisor loop for
/// testing: pure bookkeeping over child exit events.
pub struct RespawnPolicy {
    respawns: u32,
}

impl RespawnPolicy {
    pub fn new() -> Self {
        RespawnPolicy { respawns: 0 }
    }

    /// Records an unexpected exit after `uptime` of runtime and decides the
    /// next step: Some(delay) to respawn after that delay, None to give up.
    /// A long uptime first resets the counter — the child was healthy and
    /// its death is a fresh event, not a crash loop.
    pub fn on_exit(&mut self, uptime: Duration) -> Option<Duration> {
        if uptime >= STABLE_UPTIME {
            self.respawns = 0;
        }
        if self.respawns >= MAX_RESPAWNS {
            return None;
        }
        self.respawns += 1;
        Some(RESPAWN_DELAY)
    }
}

/// Spawns the indicator as a child of this daemon: our own binary with
/// `system indicator`. stdin is /dev/null; stdout/stderr are inherited so
/// the indicator's log lines fold into the daemon's log.
fn spawn_indicator() -> Result<Child> {
    let exe = std::env::current_exe()
        .context("Failed to find our own executable for spawning the tray indicator")?;
    // An auto-update may have replaced the binary on disk while we run;
    // Linux then reports our exe as "<path> (deleted)" (see main.rs).
    let exe = exe.to_string_lossy().trim_end_matches(" (deleted)").to_string();
    Command::new(exe)
        .args(["system", "indicator"])
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .context("Failed to spawn the tray indicator")
}

/// SIGTERMs the indicator child (its default disposition exits it cleanly)
/// and reaps it, escalating to SIGKILL if it doesn't die promptly. Bounded:
/// TERM_GRACE at worst.
fn terminate_and_reap(mut child: Child) {
    let pid = child.id() as i32;
    if unsafe { libc::kill(pid, libc::SIGTERM) } != 0 {
        debug!(
            "Tray indicator (pid {}) already gone: {}",
            pid,
            std::io::Error::last_os_error()
        );
    }
    let deadline = Instant::now() + TERM_GRACE;
    loop {
        match child.try_wait() {
            Ok(Some(_)) => return,
            Ok(None) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(50));
            }
            Ok(None) => {
                warn!("Tray indicator (pid {}) ignored SIGTERM, killing it", pid);
                let _ = child.kill();
                let _ = child.wait();
                return;
            }
            Err(e) => {
                debug!("Tray indicator (pid {}) reap failed: {:?}", pid, e);
                let _ = child.kill();
                let _ = child.wait();
                return;
            }
        }
    }
}

/// The supervisor's mutable state, shared between the monitor task (which
/// respawns into it), the Supervisor's Drop (which reaps out of it) and the
/// control socket's SupervisorHandle (which hides/shows). A std Child, not
/// tokio's, so Drop and hide() can reap synchronously.
struct Shared {
    /// The supervisor's own spawned child; None when hidden, not yet
    /// spawned, or given up on. NEVER a manually-started indicator.
    child: Option<Child>,
    spawned_at: Instant,
    /// "Stay down": no spawns and no respawns while set. Set by hide(), by
    /// an external SIGTERM death (takeover/manual kill — the supervisor must
    /// not fight those) and by the respawn policy giving up; cleared by
    /// show(). In-memory only: a daemon restart always starts visible.
    hidden: bool,
    /// The bounded-respawn bookkeeping; reset by show() so an explicit
    /// restore comes with a fresh budget.
    policy: RespawnPolicy,
}

impl Shared {
    /// try_wait-based liveness, reaping an exited child. Poll errors are
    /// treated as death (the handle is dropped; at worst the kernel keeps a
    /// brief zombie until the next event).
    fn child_running(&mut self) -> bool {
        match self.child.as_mut().map(|c| c.try_wait()) {
            Some(Ok(None)) => true,
            Some(Ok(Some(_))) => {
                self.child = None;
                false
            }
            Some(Err(e)) => {
                warn!("Tray indicator: failed to poll the child: {:?}", e);
                self.child = None;
                false
            }
            None => false,
        }
    }
}

/// What a show() request should do, from pre-probed facts (testable without
/// touching the environment or spawning processes).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ShowDecision {
    /// --no-indicator / MONUX_NO_INDICATOR: an explicit opt-out show must
    /// not override.
    OptedOut,
    /// No desktop session bus to talk to right now.
    Headless,
    /// A child is already running: nothing to do.
    AlreadyRunning,
    /// Clear hidden and spawn immediately.
    Spawn,
}

fn show_decision(
    veto: Option<SpawnVeto>,
    desktop_session: bool,
    child_running: bool,
) -> ShowDecision {
    if veto == Some(SpawnVeto::OptedOut) {
        return ShowDecision::OptedOut;
    }
    if !desktop_session {
        return ShowDecision::Headless;
    }
    if child_running {
        return ShowDecision::AlreadyRunning;
    }
    ShowDecision::Spawn
}

/// One monitor tick's exit observation: Some((sigterm, uptime)) when the
/// supervised child died. Hidden means supervision is dormant: the child
/// slot is not even polled (hide() already reaped what it took out).
fn observe_exit(shared: &mut Shared) -> Option<(bool, Duration)> {
    if shared.hidden {
        return None;
    }
    match shared.child.as_mut().map(|c| c.try_wait()) {
        Some(Ok(Some(status))) => {
            let uptime = shared.spawned_at.elapsed();
            shared.child = None;
            let sigterm = std::os::unix::process::ExitStatusExt::signal(&status)
                == Some(libc::SIGTERM);
            Some((sigterm, uptime))
        }
        Some(Err(e)) => {
            warn!("Tray indicator: failed to poll the child: {:?}", e);
            None
        }
        _ => None,
    }
}

/// The monitor's reaction to an observed child exit (pure state transition;
/// tested).
enum ExitAction {
    /// SIGTERM death — a takeover via the single-instance lock or a
    /// deliberate kill: it did NOT exit on its own, so don't fight it. The
    /// supervisor parks in the hidden state until show().
    TakenOver,
    /// Crash loop exhausted the respawn budget: stay down until show().
    GiveUp,
    /// Respawn after the delay (attempt number, for the log).
    RespawnAfter { delay: Duration, attempt: u32 },
}

fn exit_action(shared: &mut Shared, sigterm: bool, uptime: Duration) -> ExitAction {
    if sigterm {
        shared.hidden = true;
        return ExitAction::TakenOver;
    }
    match shared.policy.on_exit(uptime) {
        Some(delay) => ExitAction::RespawnAfter {
            delay,
            attempt: shared.policy.respawns,
        },
        None => {
            shared.hidden = true;
            ExitAction::GiveUp
        }
    }
}

/// Spawns the indicator and stores it as the supervised child; returns its
/// pid. Callers have already decided a spawn is wanted (not hidden, no live
/// child).
fn spawn_and_store(shared: &Arc<Mutex<Shared>>) -> Result<u32> {
    let child = spawn_indicator()?;
    let pid = child.id();
    let mut shared = shared.lock().unwrap();
    shared.child = Some(child);
    shared.spawned_at = Instant::now();
    Ok(pid)
}

/// The control socket's view of the supervisor: hide/show by request. Cheap
/// to clone; the Supervisor guard keeps ownership of the shutdown lifecycle.
#[derive(Clone)]
pub struct SupervisorHandle {
    shared: Arc<Mutex<Shared>>,
    shutdown: Arc<AtomicBool>,
    veto: Option<SpawnVeto>,
}

impl SupervisorHandle {
    /// `{"cmd":"indicator","action":"hide"}`: stop the spawned child (if
    /// any) and suppress spawns/respawns until show(). Idempotent; never
    /// touches a manually-started indicator.
    pub fn hide(&self) {
        let child = {
            let mut shared = self.shared.lock().unwrap();
            shared.hidden = true;
            shared.child.take()
        };
        if let Some(child) = child {
            info!("Hiding the tray indicator on request");
            terminate_and_reap(child);
        }
    }

    /// `{"cmd":"indicator","action":"show"}`: clear hidden and spawn
    /// immediately when no child is running. Refuses to override an explicit
    /// --no-indicator opt-out, and fails clearly in headless sessions.
    pub fn show(&self) -> Result<()> {
        if self.shutdown.load(Ordering::Relaxed) {
            bail!("the daemon is shutting down");
        }
        let child_running = self.shared.lock().unwrap().child_running();
        match show_decision(self.veto, has_desktop_session(), child_running) {
            ShowDecision::OptedOut => bail!(
                "the daemon was started with --no-indicator (or MONUX_NO_INDICATOR), an explicit opt-out — restart the daemon without it to enable the tray icon"
            ),
            ShowDecision::Headless => bail!(
                "no D-Bus session bus: the indicator needs a desktop session running a StatusNotifierItem host (waybar, KDE Plasma, ...)"
            ),
            ShowDecision::AlreadyRunning => Ok(()),
            ShowDecision::Spawn => {
                let pid = spawn_and_store(&self.shared)?;
                // A fresh start after give-up/takeover gets a fresh budget.
                let mut shared = self.shared.lock().unwrap();
                shared.policy = RespawnPolicy::new();
                shared.hidden = false;
                info!("Showed the tray indicator on request (pid {})", pid);
                Ok(())
            }
        }
    }
}

/// Guard for the auto-spawned tray indicator. Created early (new) so the
/// control socket gets a handle, launched once the daemon is up (launch:
/// server listening, client control socket bound) and kept alive; on drop —
/// every daemon exit path unwinds past it — the child is SIGTERM'd and
/// reaped.
pub struct Supervisor {
    shared: Arc<Mutex<Shared>>,
    shutdown: Arc<AtomicBool>,
    veto: Option<SpawnVeto>,
}

impl Supervisor {
    /// Resolves the auto-spawn veto; spawns nothing and starts no task yet
    /// (that is launch(), once the daemon is up).
    pub fn new(no_indicator_flag: bool) -> Supervisor {
        Supervisor {
            shared: Arc::new(Mutex::new(Shared {
                child: None,
                spawned_at: Instant::now(),
                hidden: false,
                policy: RespawnPolicy::new(),
            })),
            shutdown: Arc::new(AtomicBool::new(false)),
            veto: spawn_veto(
                no_indicator_flag,
                std::env::var_os("MONUX_NO_INDICATOR").is_some(),
                probe_session_bus_env(),
                probe_user_bus_socket(),
            ),
        }
    }

    /// A handle for the control socket's indicator command.
    pub fn handle(&self) -> SupervisorHandle {
        SupervisorHandle {
            shared: self.shared.clone(),
            shutdown: self.shutdown.clone(),
            veto: self.veto,
        }
    }

    /// Spawns the indicator (unless vetoed) and starts the monitor task on
    /// the current tokio runtime. Call once, on the runtime, when the daemon
    /// is up. Even when the veto suppresses the initial spawn (headless),
    /// the monitor runs so a later control-socket show() is supervised —
    /// except on an explicit opt-out, where show() is refused anyway.
    pub fn launch(&self) {
        match self.veto {
            Some(SpawnVeto::OptedOut) => {
                debug!("Tray indicator auto-spawn disabled (--no-indicator / MONUX_NO_INDICATOR)");
                return;
            }
            Some(SpawnVeto::NoDesktopSession) => {
                debug!("No desktop session bus, not auto-spawning the tray indicator");
            }
            None => match spawn_and_store(&self.shared) {
                Ok(pid) => info!("Spawned the tray indicator (pid {}, --no-indicator to disable)", pid),
                // Auxiliary feature: never fail the daemon over it. A later
                // 'tray show' retries.
                Err(e) => warn!("Failed to spawn the tray indicator: {:#}", e),
            },
        }
        tokio::task::spawn(monitor_loop(self.shared.clone(), self.shutdown.clone()));
    }
}

impl Drop for Supervisor {
    /// Graceful daemon shutdown: SIGTERM the indicator and reap it.
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        let child = self.shared.lock().unwrap().child.take();
        if let Some(child) = child {
            terminate_and_reap(child);
        }
    }
}

/// Watches the supervised child and respawns it on unexpected exit, bounded
/// by the RespawnPolicy in the shared state. Dormant while hidden; ends when
/// the Supervisor's Drop sets `shutdown` (the Drop has already reaped the
/// child by then).
async fn monitor_loop(shared: Arc<Mutex<Shared>>, shutdown: Arc<AtomicBool>) {
    loop {
        tokio::time::sleep(POLL_INTERVAL).await;
        if shutdown.load(Ordering::Relaxed) {
            return;
        }
        // The lock is never held across an await: Drop and hide()/show()
        // need it for their own child operations.
        let exited = { observe_exit(&mut shared.lock().unwrap()) };
        let Some((sigterm, uptime)) = exited else { continue };
        let action = { exit_action(&mut shared.lock().unwrap(), sigterm, uptime) };
        let (delay, attempt) = match action {
            ExitAction::TakenOver => {
                info!(
                    "Tray indicator was terminated (takeover or kill); staying down until 'monux system tray show'"
                );
                continue;
            }
            ExitAction::GiveUp => {
                warn!(
                    "Tray indicator keeps exiting (giving up after {} respawns) — restore it with 'monux system tray show' or 'monux system indicator'",
                    MAX_RESPAWNS
                );
                continue;
            }
            ExitAction::RespawnAfter { delay, attempt } => (delay, attempt),
        };
        info!(
            "Tray indicator exited on its own after {}s; respawning in {}s (attempt {}/{})",
            uptime.as_secs(),
            delay.as_secs(),
            attempt,
            MAX_RESPAWNS
        );
        // Sleep out the delay in small slices so a shutdown or a hide/show
        // isn't held up by it.
        let deadline = Instant::now() + delay;
        while Instant::now() < deadline {
            tokio::time::sleep(POLL_INTERVAL).await;
            if shutdown.load(Ordering::Relaxed) {
                return;
            }
            if shared.lock().unwrap().hidden {
                break;
            }
        }
        {
            let mut shared = shared.lock().unwrap();
            // Hidden (or shown, which spawns directly) during the delay?
            if shared.hidden || shared.child.is_some() {
                continue;
            }
            match spawn_indicator() {
                Ok(child) => {
                    info!("Respawned the tray indicator (pid {})", child.id());
                    shared.child = Some(child);
                    shared.spawned_at = Instant::now();
                }
                Err(e) => {
                    shared.hidden = true;
                    warn!(
                        "Failed to respawn the tray indicator: {:#} — restore it with 'monux system tray show' or 'monux system indicator'",
                        e
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn veto_opted_out_by_flag_or_env() {
        // Either opt-out wins over an available session.
        assert_eq!(
            spawn_veto(true, false, true, true),
            Some(SpawnVeto::OptedOut)
        );
        assert_eq!(
            spawn_veto(false, true, true, true),
            Some(SpawnVeto::OptedOut)
        );
        // Opt-out is reported even when there is no session at all.
        assert_eq!(
            spawn_veto(true, false, false, false),
            Some(SpawnVeto::OptedOut)
        );
    }

    #[test]
    fn veto_headless_session() {
        // Neither the env var nor the bus socket: no spawn.
        assert_eq!(
            spawn_veto(false, false, false, false),
            Some(SpawnVeto::NoDesktopSession)
        );
        // Either probe alone is enough to spawn.
        assert_eq!(spawn_veto(false, false, true, false), None);
        assert_eq!(spawn_veto(false, false, false, true), None);
        assert_eq!(spawn_veto(false, false, true, true), None);
    }

    #[test]
    fn respawn_policy_allows_bounded_respawns_then_gives_up() {
        let mut policy = RespawnPolicy::new();
        let crash = Duration::from_secs(5);
        for _ in 0..MAX_RESPAWNS {
            assert_eq!(policy.on_exit(crash), Some(RESPAWN_DELAY));
        }
        assert_eq!(policy.on_exit(crash), None);
        // Terminal: further exits keep returning None.
        assert_eq!(policy.on_exit(crash), None);
    }

    #[test]
    fn respawn_policy_resets_after_stable_uptime() {
        let mut policy = RespawnPolicy::new();
        let crash = Duration::from_secs(5);
        // Two quick crashes, then a long stable run resets the counter...
        assert_eq!(policy.on_exit(crash), Some(RESPAWN_DELAY));
        assert_eq!(policy.on_exit(crash), Some(RESPAWN_DELAY));
        assert_eq!(policy.on_exit(STABLE_UPTIME), Some(RESPAWN_DELAY));
        // ...so the full budget is available again afterwards.
        assert_eq!(policy.on_exit(crash), Some(RESPAWN_DELAY));
        assert_eq!(policy.on_exit(crash), Some(RESPAWN_DELAY));
        assert_eq!(policy.on_exit(crash), None);
    }

    #[test]
    fn respawn_policy_boundary_uptime_is_stable() {
        // Exactly at the boundary counts as stable: the counter resets, so
        // the full budget is available afterwards.
        let mut policy = RespawnPolicy::new();
        assert_eq!(policy.on_exit(STABLE_UPTIME), Some(RESPAWN_DELAY));
        assert_eq!(policy.on_exit(Duration::ZERO), Some(RESPAWN_DELAY));
        assert_eq!(policy.on_exit(Duration::ZERO), Some(RESPAWN_DELAY));
        assert_eq!(policy.on_exit(Duration::ZERO), None);
        // Just below the boundary does not reset.
        let mut policy = RespawnPolicy::new();
        assert_eq!(
            policy.on_exit(STABLE_UPTIME - Duration::from_nanos(1)),
            Some(RESPAWN_DELAY)
        );
        assert_eq!(policy.on_exit(Duration::ZERO), Some(RESPAWN_DELAY));
        assert_eq!(policy.on_exit(Duration::ZERO), Some(RESPAWN_DELAY));
        assert_eq!(policy.on_exit(Duration::ZERO), None);
    }

    #[test]
    fn show_decision_matrix() {
        // The explicit opt-out is sticky: no session or running child
        // changes it.
        assert_eq!(
            show_decision(Some(SpawnVeto::OptedOut), true, false),
            ShowDecision::OptedOut
        );
        assert_eq!(
            show_decision(Some(SpawnVeto::OptedOut), true, true),
            ShowDecision::OptedOut
        );
        // Headless (also the launch-veto) blocks a show, re-probed live.
        assert_eq!(
            show_decision(Some(SpawnVeto::NoDesktopSession), false, false),
            ShowDecision::Headless
        );
        assert_eq!(show_decision(None, false, false), ShowDecision::Headless);
        // A headless-at-launch veto does NOT block a show once a session
        // exists: the session may have appeared after the daemon started.
        assert_eq!(
            show_decision(Some(SpawnVeto::NoDesktopSession), true, false),
            ShowDecision::Spawn
        );
        // Already running: no-op; otherwise spawn.
        assert_eq!(show_decision(None, true, true), ShowDecision::AlreadyRunning);
        assert_eq!(show_decision(None, true, false), ShowDecision::Spawn);
    }

    #[test]
    fn exit_action_marks_hidden_on_takeover_and_give_up() {
        let mut shared = shared_for_test();
        // SIGTERM death (takeover/manual kill): parked hidden, no respawn.
        let action = exit_action(&mut shared, true, Duration::from_secs(5));
        assert!(matches!(action, ExitAction::TakenOver));
        assert!(shared.hidden);
        // Ordinary crashes: bounded respawns, then GiveUp parks hidden.
        let mut shared = shared_for_test();
        for attempt in 1..=MAX_RESPAWNS {
            match exit_action(&mut shared, false, Duration::from_secs(5)) {
                ExitAction::RespawnAfter { delay, attempt: n } => {
                    assert_eq!(delay, RESPAWN_DELAY);
                    assert_eq!(n, attempt);
                }
                _ => panic!("attempt {} must respawn", attempt),
            }
            assert!(!shared.hidden);
        }
        let action = exit_action(&mut shared, false, Duration::from_secs(5));
        assert!(matches!(action, ExitAction::GiveUp));
        assert!(shared.hidden);
    }

    #[test]
    fn observe_exit_is_dormant_while_hidden() {
        let mut shared = shared_for_test();
        // A dead child sits in the slot, unreaped...
        shared.child = Some(spawn_sh("exit 0"));
        shared.spawned_at = Instant::now() - Duration::from_secs(1);
        std::thread::sleep(Duration::from_millis(100));
        // ...but hidden means dormant: the monitor does not even poll it.
        shared.hidden = true;
        assert!(observe_exit(&mut shared).is_none());
        assert!(shared.child.is_some());
        // Unhidden, the death is observed and classified (exit 0: not a
        // SIGTERM death).
        shared.hidden = false;
        let (sigterm, uptime) = observe_exit(&mut shared).expect("death must be observed");
        assert!(!sigterm);
        assert!(uptime >= Duration::from_secs(1));
        assert!(shared.child.is_none());
    }

    #[test]
    fn hide_takes_and_reaps_the_child() {
        let shared = Arc::new(Mutex::new(Shared {
            child: Some(spawn_sh("sleep 30")),
            spawned_at: Instant::now(),
            hidden: false,
            policy: RespawnPolicy::new(),
        }));
        let handle = SupervisorHandle {
            shared: shared.clone(),
            shutdown: Arc::new(AtomicBool::new(false)),
            veto: None,
        };
        // hide: marks hidden and reaps the spawned child promptly.
        handle.hide();
        {
            let mut shared = shared.lock().unwrap();
            assert!(shared.hidden);
            assert!(!shared.child_running());
        }
        // hide is idempotent with no child.
        handle.hide();
        assert!(shared.lock().unwrap().hidden);
        // An opted-out handle refuses show (before any probe/spawn)...
        let opted_out = SupervisorHandle {
            shared: shared.clone(),
            shutdown: Arc::new(AtomicBool::new(false)),
            veto: Some(SpawnVeto::OptedOut),
        };
        let err = opted_out.show().unwrap_err().to_string();
        assert!(err.contains("--no-indicator"), "{}", err);
        // ...and a shutting-down supervisor refuses it too.
        let shutting_down = SupervisorHandle {
            shared: shared.clone(),
            shutdown: Arc::new(AtomicBool::new(true)),
            veto: None,
        };
        assert!(shutting_down.show().is_err());
        // (The show-spawns-a-fresh-child path needs a real monux binary and
        // a session bus; it is covered by the E2E tests.)
    }

    /// Shared state as the monitor sees it between events.
    fn shared_for_test() -> Shared {
        Shared {
            child: None,
            spawned_at: Instant::now(),
            hidden: false,
            policy: RespawnPolicy::new(),
        }
    }

    /// A real child process standing in for the indicator (spawn/try_wait/
    /// SIGTERM semantics are the same), without dragging in the session bus.
    fn spawn_sh(script: &str) -> Child {
        Command::new("sh")
            .arg("-c")
            .arg(script)
            .stdin(Stdio::null())
            .spawn()
            .expect("sh must spawn")
    }
}
