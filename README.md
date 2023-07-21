# nikau

[![builds.sr.ht status](https://builds.sr.ht/~nickbp/nikau/commits/main/.build.yml.svg)](https://builds.sr.ht/~nickbp/nikau/commits/main/.build.yml)

```
\\ //
 \V/
  U
  |
  | nikau
```

TLS-encrypted server-client KVM software for sharing input devices across Linux machines.

## How it works

Nikau relies on the Linux uinput API, and supports Wayland, X11, and plain Linux consoles. OSX and Windows are not supported.

It is packaged as a single executable which supports both server and client modes.

Input devices are connected to the server, which sends nput events to a selected client. Clients receive and emit input events from the server using virtual uinput devices.

When a key is pressed or mouse is moved on the server, Nikau will encode and send the event over the network to a selected client, if any. That client will then write the event to a virtual device, to be picked up by the OS.

Key combinations are used to rotate between machines. The default is `LeftAlt+N` to go to the next machine, or `LeftAlt+P` to go to the previous machine. This is customized using commandline arguments on the server.

## Install

There are multiple ways to get `nikau` installed on your client and server machines:

### Building a stable release

1. Install cargo
2. Run `cargo install nikau`.
3. Use `sudo ~/.cargo/bin/nikau` to run the binary. `sudo` is required to allow access to uinput.

### Building from latest `main`

1. `git clone https://git.sr.ht/~nickbp/nikau`
2. See `server.sh` and `client.sh <server-host>` for example usage. The scripts use `sudo` to allow access to uinput.

### Running the Docker image

1. Get a list of available tags (based on commit SHAs) from [here](https://github.com/users/nickbp/packages/container/package/nikau).
2. See `docker-server.sh <tag>` and `docker-client.sh <tag> <server-host>` for example `docker run` commands. The commands use `--privileged` in order to allow access to uinput.

## Getting started

You need to start a nikau instance on each machine:
- On your server (have keyboard/mouse/touchpad): Run `nikau server` using one of the methods listed above.
- On your clients (accept remote input): Run `nikau client <server-host>` using one of the methods listed above.

Additional optional arguments can be found via `nikau <mode> -h`. They shouldn't be needed for typical usage.

When a client connects to a server for the first time, you will need to approve the certificate handshake on _both_ the server _and_ the client. Check the displayed fingerprints on each side and confirm they line up with the displayed fingerprints on the other side. Similar to SSH, this manual approval process is only required for the first connection, after that the certificates are "known". You can check a server or client's cert fingerprint directly by running `sudo openssl x509 -noout -sha256 -fingerprint -in /root/.config/nikau/private.pem`. You can also use the `--fingerprints` argument to preapprove any known fingerprints without the prompt. For safety reasons, the fingerprint check cannot be disabled.

Once connections have been approved on both sides, the server should log that it's added the client to its rotation, and the client should log that it's waiting to be activated. This is the default state, where input on the server is staying local to the server. The KVM isn't doing anything yet.

To send input from the server to connected client(s), try pressing `LeftAlt + N` and `LeftAlt + P` to rotate forward and backward between clients and the server. For now, clients are simply ordered alphabetically by their IP/port endpoint. These shortcuts may be customized using `--shortcut` and `--shortcut-prev` at the server. External tooling can send `SIGUSR1` and `SIGUSR2` signals to the server process to trigger forward and backward rotation, respectively.

## Project status

I'm using this on a regular basis. As such it should "just work", but there may be issues with certain input devices that I haven't tried yet. Email me with info if you're having problems.

The wire protocol is still very unstable and will frequently change between releases for a while. For now, you should ensure that all servers and clients are running the same build.

Plans/known shortcomings:
- Clipboards are not synced across devices. I would like support for this. Currently, Nikau doesn't have access to clipboard contents as it only interfaces with uinput, but if e.g. Wayland offers an interface for this then I don't think there's a problem supporting it.
- I'd like to figure out access to uinput without requiring full `sudo`. The server at least needs continuing access so that it can monitor new input devices that are plugged in while the server is running. The client might be able to downgrade its access after creating virtual devices on startup.
- Nikau does not work on OSX or Windows, and I don't have any plans to add support for them.

## Security

**This software has NOT undergone any security review or audit. Use is at your own risk.** See also terms and conditions of the [licence](LICENCE.md).

Keep in mind that the purpose of this software is to essentially collect keystrokes and send them over the network to another machine. Whether this is acceptable is something that you must decide based on your own risk assessment.

The communication containing user input data should be TLS-encrypted, assuming there aren't flagrant bugs in either Nikau or the many underlying libraries it depends on. Authentication requires bidirectional user approval and follows a "prompt-once" model using self-signed server and client certificates.

You should keep `nikau` communication off of public networks. For example, the software does not try to hide the timing of input, so it's conceivable that an outsider could infer information about user input by watching the rate of traffic.

In order to have access to uinput devices, the client and server must both be run as root (e.g. via `sudo`).

## License

This project is [licensed](LICENCE.md) under the AGPLv3 (or later) and is copyright Nicholas Parker.
