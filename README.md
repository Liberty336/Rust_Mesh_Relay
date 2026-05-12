# Rust_Mesh_Relay
EXPERIMENTAL, IN DEVELOPMENT<br>
Post-disaster mesh messenger. No internet required, just WiFi range.

Rust_Mesh_Relay is a cross platform relay program for post-disaster situations.<br>
Assuming you had the ability to recharge your electronics (via solar panels), but lacked internet,<br>
this program should help you establish an emergency communications network among your people via a mesh.<br>
                                                                                                          
## What's inside
Here's what's inside and why each piece exists:<br>

packet.rs: the wire format. Same design philosophy as our original C protocol: manual byte serialization, big-endian, additive checksum. Added origin_id, dest_id, packet_id, and hop_count, the four fields that make flooding with deduplication possible.<br>

node.rs: shared state wrapped in Arc<Mutex<Node>>.<br>
Three threads all need access to the neighbor table and seen-packet set, so it lives here behind a lock.<br>
<br>
main.rs: three threads talking to each other:
<br><br>
1. receive thread -> blocks on recv_from(), processes every packet, forwards everything new<br>
2. announce thread -> wakes every 5 seconds, broadcasts "I'm here"<br>
3. main thread -> reads stdin, handles commands<br>
<br><br>
The key design choice is collecting actions while holding the mutex, then releasing the lock before doing any I/O sending, printing, saving files. Holding a mutex while doing a syscall is a classic deadlock risk.<br><br>

### Android note: 
install Termux from F-Droid not the Play Store<br>
The Play Store version is years out of date and `pkg install rust` won't work on it.


## Features
Text messaging <br><br>

Type anything and hit Enter -> broadcasts to everyone on the network<br>
Messages show the sender's name<br>
Relayed automatically through intermediate devices
<br><br>
File transfer? Yes, including video!
<br><br>
/sendfile <path> sends any file: photos, videos, documents, anything<br>
Works by splitting the file into 1KB chunks, each sent as a separate UDP packet<br>
Receivers collect chunks and reassemble when all arrive<br>
Saved automatically to a received_files/ folder<br>
<br><br>
Peer discovery
<br><br>
Every device announces itself every 5 seconds automatically<br>
/peers lists everyone currently known on the network<br>
New arrivals print a notification when first seen<br>
<br><br>
Mesh relaying
<br><br>
Packets hop through intermediate devices automatically<br>
Max 8 hops — so Alice can reach Charlie through Bob even if Alice and Charlie can't directly see each other<br>
Deduplication prevents the same packet from circling forever<br>

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



## Honest Limitations
It works but has real problems for large files: <br><br>

### No retransmission: UDP drops packets silently.<br>
If one chunk out of 500 is lost, the whole file is incomplete and there's no mechanism to re-request it. <br>
For a 10MB photo this is usually fine. For a 100MB video it will likely fail.<br>

### No progress on the receiver side: the sender sees progress, but the receiver just waits silently until all chunks arrive (or doesn't, if they don't).
<br><br>

### Speed: there's a 5ms delay between chunks to avoid flooding relays.<br>
A 50MB video is ~50,000 chunks, so roughly 4 minutes minimum transfer time.<br><br>


## Next Steps
The right next feature to add would be a chunk acknowledgment system, the receiver tracks which chunks it has, and after the transfer, broadcasts a list of missing ones so the sender can retransmit only those.<br><br>
That would make video transfer genuinely reliable.
