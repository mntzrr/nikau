//! Development probe: injects key events into a physical input device and
//! reads back what arrives on another device (e.g. monux's virtual keyboard).
//! The output device is grabbed during the probe so no injected events can
//! leak to the compositor.
//!
//! Usage: enter_probe <physical-name-substring> <virtual-name-substring> [keycode...]
//! Default injected keys: KEY_A (30) and KEY_ENTER (28).

use std::fs;
use std::io::Write;
use std::os::unix::io::AsRawFd;

const EV_KEY: u16 = 0x01;
const EV_SYN: u16 = 0x00;
const SYN_REPORT: u16 = 0x00;

#[repr(C)]
#[derive(Clone, Copy)]
struct RawInputEvent {
    sec: i64,
    usec: i64,
    type_: u16,
    code: u16,
    value: i32,
}

fn write_raw_event(f: &mut fs::File, type_: u16, code: u16, value: i32) {
    let ev = RawInputEvent {
        sec: 0,
        usec: 0,
        type_,
        code,
        value,
    };
    let bytes = unsafe { std::slice::from_raw_parts(&ev as *const _ as *const u8, 24) };
    f.write_all(bytes).expect("failed to write input event");
}

fn write_key(f: &mut fs::File, code: u16) {
    write_raw_event(f, EV_KEY, code, 1);
    write_raw_event(f, EV_SYN, SYN_REPORT, 0);
    write_raw_event(f, EV_KEY, code, 0);
    write_raw_event(f, EV_SYN, SYN_REPORT, 0);
}

fn find_device(substr: &str) -> String {
    for entry in fs::read_dir("/dev/input").expect("failed to list /dev/input") {
        let entry = entry.expect("failed to read /dev/input entry");
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if !name.starts_with("event") {
            continue;
        }
        let path = entry.path();
        let Ok(device) = evdev::Device::open(&path) else {
            continue;
        };
        if device
            .name()
            .map(|n| n.contains(substr))
            .unwrap_or(false)
        {
            return path.to_string_lossy().to_string();
        }
    }
    panic!("no input device matching '{}' found", substr);
}

/// Prints every event received on matching devices for `secs` seconds.
/// Usage: enter_probe --listen <name-substring-or-"all"> <secs>
fn listen(substr: &str, secs: u64) {
    let mut fds: Vec<(String, fs::File)> = vec![];
    for entry in fs::read_dir("/dev/input").expect("failed to list /dev/input") {
        let entry = entry.expect("failed to read /dev/input entry");
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if !name.starts_with("event") {
            continue;
        }
        let path = entry.path();
        let Ok(device) = evdev::Device::open(&path) else {
            continue;
        };
        let dev_name = device.name().unwrap_or("").to_string();
        if substr != "all" && !dev_name.contains(substr) {
            continue;
        }
        let Ok(f) = fs::OpenOptions::new().read(true).open(&path) else {
            continue;
        };
        fds.push((format!("{} ({})", dev_name, path.display()), f));
    }
    println!("listening on {} devices for {}s; press keys now:", fds.len(), secs);
    for (name, _) in &fds {
        println!("  {}", name);
    }
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(secs);
    let mut pfds: Vec<libc::pollfd> = fds
        .iter()
        .map(|(_, f)| libc::pollfd {
            fd: f.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        })
        .collect();
    while std::time::Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        let ret = unsafe { libc::poll(pfds.as_mut_ptr(), pfds.len() as libc::nfds_t, remaining.as_millis() as i32) };
        if ret <= 0 {
            break;
        }
        for (i, pfd) in pfds.iter_mut().enumerate() {
            if pfd.revents & libc::POLLIN == 0 {
                continue;
            }
            let mut buf = [0u8; 24 * 16];
            let n = unsafe { libc::read(pfd.fd, buf.as_mut_ptr() as *mut libc::c_void, buf.len()) };
            if n <= 0 {
                continue;
            }
            let mut off = 0;
            while off + 24 <= n as usize {
                let ev: RawInputEvent =
                    unsafe { std::ptr::read(buf.as_ptr().add(off) as *const RawInputEvent) };
                off += 24;
                if ev.type_ == EV_SYN {
                    continue;
                }
                println!(
                    "{}: type={} code={} value={}",
                    fds[i].0, ev.type_, ev.code, ev.value
                );
            }
        }
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.get(1).map(|s| s == "--listen").unwrap_or(false) {
        let substr = args.get(2).map(|s| s.as_str()).unwrap_or("all");
        let secs: u64 = args
            .get(3)
            .and_then(|s| s.parse().ok())
            .unwrap_or(10);
        listen(substr, secs);
        return;
    }
    let physical_name = args.get(1).expect("usage: enter_probe <physical-name-substring> <virtual-name-substring> [keycode...]");
    let virtual_name = args.get(2).expect("usage: enter_probe <physical-name-substring> <virtual-name-substring> [keycode...]");
    let keycodes: Vec<u16> = if args.len() > 3 {
        args[3..]
            .iter()
            .map(|s| s.parse().expect("keycode must be a number"))
            .collect()
    } else {
        vec![30, 28]
    };

    let physical_path = find_device(physical_name);
    let virtual_path = find_device(virtual_name);
    println!("physical device: {}", physical_path);
    println!("virtual device:  {}", virtual_path);

    // Grab the virtual device so injected events can't leak to the compositor.
    let mut virtual_device = evdev::Device::open(&virtual_path).expect("failed to open virtual device");
    virtual_device.grab().expect("failed to grab virtual device");
    println!("grabbed virtual device (events will NOT reach the compositor)");

    let mut physical = fs::OpenOptions::new()
        .write(true)
        .open(&physical_path)
        .expect("failed to open physical device for writing");

    // Read events already pending on the virtual device, then inject.
    let vfd = virtual_device.as_raw_fd();
    let drain = |label: &str| {
        let mut pfd = libc::pollfd {
            fd: vfd,
            events: libc::POLLIN,
            revents: 0,
        };
        let mut seen: Vec<(u16, u16, i32)> = vec![];
        loop {
            let ret = unsafe { libc::poll(&mut pfd, 1, 300) };
            if ret <= 0 {
                break;
            }
            let mut buf = [0u8; 24];
            let n = unsafe { libc::read(vfd, buf.as_mut_ptr() as *mut libc::c_void, 24) };
            if n != 24 {
                break;
            }
            let ev: RawInputEvent = unsafe { std::ptr::read(buf.as_ptr() as *const RawInputEvent) };
            seen.push((ev.type_, ev.code, ev.value));
        }
        println!("{}: {} events", label, seen.len());
        seen
    };

    // Drain anything pending before we start.
    let _ = drain("pending");

    let mut results: Vec<(u16, bool)> = vec![];
    for code in &keycodes {
        println!("injecting keycode {} into physical device...", code);
        write_key(&mut physical, *code);
        let events = drain("received");
        let presses = events
            .iter()
            .filter(|(t, c, v)| *t == EV_KEY && *c == *code && *v == 1)
            .count();
        let releases = events
            .iter()
            .filter(|(t, c, v)| *t == EV_KEY && *c == *code && *v == 0)
            .count();
        println!(
            "  keycode {}: press={} release={} (all events: {:?})",
            code, presses, releases, events
        );
        results.push((*code, presses > 0 && releases > 0));
    }

    virtual_device.ungrab().expect("failed to ungrab virtual device");
    println!("ungrabbed virtual device");

    let mut failed = false;
    for (code, ok) in &results {
        println!("keycode {}: {}", code, if *ok { "OK" } else { "MISSING" });
        if !ok {
            failed = true;
        }
    }
    std::process::exit(if failed { 1 } else { 0 });
}
