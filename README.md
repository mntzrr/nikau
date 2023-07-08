# nikau

```
\\ //
 \V/
  U
  |
  | nikau
```

[![builds.sr.ht status](https://builds.sr.ht/~nickbp/nikau/commits/main/.build.yml.svg)](https://builds.sr.ht/~nickbp/nikau/commits/main/.build.yml)

TLS-encrypted server-client KVM software for sharing input devices across Linux machines.

## How it works

Nikau relies on the Linux uinput API, and supports Wayland, X11, and plain Linux consoles.

It is packaged as a single executable which supports both server and client modes.

The server is where the input devices are plugged in, while the clients are remotely controlled by the server.

When a key is pressed or mouse is moved on the server, Nikau will encode and send the event over the network to the currently enabled client (if any). The client will then write the event to a virtual device, to be picked up by the host OS.

Key combinations are used to rotate between machines. The default is Alt+N to go to the next machine, or Alt+P to go to the previous machine. This can be configured via commandline arguments on the server.

## Quickstart

On the server and on the client(s):
```
cargo install nikau
```

On the server:
```
sudo ~/.cargo/bin/nikau server
```

On the client(s):
```
sudo ~/.cargo/bin/nikau client <serverIP>
```

When a client connects to a server for the first time, you will need to approve the certificate handshake on _both_ the server _and_ the client. Check the displayed `Server fingerprint` and `Client fingerprint` and confirm they look the same across the server and client machines. Similar to SSH, this manual approval process is only required for the first connection, after that the certificates are "known". You can check a server or client's cert fingerprint directly by running `sudo openssl x509 -noout -sha256 -fingerprint -in /root/.config/nikau/private.pem`.

Once things have connected, the server should log that it's added the client to its rotation, and the client should log that it's waiting to be activated. This is the default state, where input on the server is staying local to the server. The KVM isn't doing anything yet.

To send input from the server to the connected client(s), try pressing **`Alt+N`** and **`Alt+P`** to rotate forward and backward between clients and the server. For now, clients are simply ordered alphabetically by their IP/port. These shortcuts are configurable at the server. Another option is sending `SIGUSR1` and `SIGUSR2` signals to the server process, which will also trigger forward and backward rotation.


## Status

I'm using this on a regular basis. As such it should "just work". Email me if you're having problems.

Known shortcomings:
- Touchpads have partial support, the behavior is wrong when you lift your finger and put it back on the touchpad. There's probably an issue with drivers. Mice and keyboards meanwhile seem to work fine across a variety of models and types.
- Clipboards are not synced across devices. I would like support for this. Currently, Nikau doesn't have access to clipboard contents as it only interfaces with uinput, but if e.g. Wayland offers an interface for this then I don't think there's a problem supporting it.

## Security

**This software has NOT undergone a security review or audit. As such, use is at your own risk.**

Keep in mind that the purpose of this software is to essentially collect keystrokes and send them over the network to another machine. Whether this is acceptable is something that you must decide based on your context and use case.

_Assuming_ there aren't flagrant security flaws in either Nikau or the underlying libraries that it depends upon, the communication containing user input data should be TLS-encrypted. Authentication uses self-signed server and client certificates, following a "prompt once" model. On the first connection, manual bidirectional approval is required on both the server and the client.

In order to have access to uinput devices, the client and server need to be run as root (e.g. via `sudo`).

## License

This project is [licensed](LICENCE.md) under the AGPLv3 and is copyright Nicholas Parker.
