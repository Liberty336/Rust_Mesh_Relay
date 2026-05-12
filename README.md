# Rust_Relay
EXPERIMENTAL, IN DEVELOPMENT<br>
Post-disaster mesh messenger. No internet required, just WiFi range.

## How it works

Every device broadcasts UDP packets on the local network (or hotspot).<br>
Devices out of direct range are reached via relay:
<br>
```
[Alice] ──WiFi──> [Bob] ──WiFi──> [Charlie]
                    ↑
                 relay node
```
<br>
Packets carry a hop counter. Each relay increments it.<br>
At max_hops (8), the packet is dropped, which prevents infinite loops.
<br><br>

You can extend the amount of hops taken for larger mesh networks by changing source code

## Build & run

### Linux / Mac / Windows

```bash
# Install Rust if you haven't:
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Clone and build:
cargo build --release

# Run:
./target/release/rust_relay YourName
```

### Android (via Termux)

```bash
# 1. Install Termux from F-Droid (NOT the Play Store version — it's outdated)
#    https://f-droid.org/en/packages/com.termux/

# 2. In Termux:
pkg update && pkg install rust

# 3. Clone and build (takes a few minutes on a phone):
cargo build --release

# 4. Run:
./target/release/rust_relay YourName
```

## Usage

```
rust_relay <name> [port]

  rust_relay Alice           # uses default port 37760
  rust_relay Bob 37761       # custom port (if two instances on same machine)
```

### Commands while running

```
hello everyone          → broadcasts "hello everyone" to all devices
/peers                  → list discovered devices
/sendfile photo.jpg     → send a file to everyone (saved to received_files/)
/quit                   → exit
```

## Networking requirements

- All devices must be on the **same WiFi network** or connected to the **same phone hotspot**
- The network must allow UDP broadcast (most hotspots do; some enterprise WiFi blocks it)
- No internet connection needed — purely local RF

## Limitations (known, fixable)

- **No reliability**: UDP has no delivery guarantee. Packets can be lost.
  File transfer has no retransmission — if a chunk is lost, the file is incomplete.
- **No encryption**: messages are plaintext on the local network.
- **No persistence**: device ID is regenerated each run.
- **Broadcast only**: text goes to everyone, no private messaging yet.

## Packet format

```
┌────────┬─────────┬──────────┬──────────┬──────────┬─────────┬──────────┬─────────────┬──────────┬─────────┐
│ magic  │ version │ msg_type │ origin   │ dest     │ pkt_id  │ hops/max │ payload_len │ checksum │ payload │
│ "RLAY" │ 1       │ 1 byte   │ 16 bytes │ 16 bytes │ 4 bytes │ 1B + 1B  │ 2 bytes     │ 4 bytes  │ ...     │
└────────┴─────────┴──────────┴──────────┴──────────┴─────────┴──────────┴─────────────┴──────────┴─────────┘
Total header: 50 bytes
```
