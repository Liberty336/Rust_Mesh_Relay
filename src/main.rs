//! Rust_Relay — post-disaster mesh messenger.
//!
//! Usage: rust_relay <your_name> [port]
//!   e.g: rust_relay Alice
//!        rust_relay Bob 37761
//!
//! All devices on the same WiFi network (or hotspot) automatically discover
//! each other. Messages are relayed hop-by-hop so devices out of direct range
//! can still communicate.
//!
//! Commands (while running):
//!   Just type + Enter     → broadcast a text message to everyone
//!   /peers                → list discovered devices
//!   /sendfile <path>      → send a file (photo, video, document) to everyone
//!   /quit                 → exit

mod packet;
mod node;

use std::fs;
use std::io::{self, BufRead};
use std::net::UdpSocket;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use packet::*;
use node::Node;

// ── Config ────────────────────────────────────────────────────────────────────

const BROADCAST_ADDR:      &str      = "255.255.255.255";
const ANNOUNCE_INTERVAL:   Duration  = Duration::from_secs(5);
const RECV_BUF_SIZE:       usize     = 2048; // max UDP datagram we handle

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Format a DeviceId as a hex string (for display).
pub fn hex_id(id: &DeviceId) -> String {
    id.iter().map(|b| format!("{:02x}", b)).collect()
}

/// Short ID: first 8 hex chars — enough to distinguish devices in a local mesh.
pub fn short_id(id: &DeviceId) -> String {
    hex_id(id)[..8].to_string()
}

/// Send a packet via UDP broadcast.
fn broadcast(socket: &UdpSocket, packet: &Packet, port: u16) {
    let bytes = packet.to_bytes();
    let addr  = format!("{}:{}", BROADCAST_ADDR, port);
    // Ignore send errors — in a real implementation, log or retry.
    let _ = socket.send_to(&bytes, addr);
}

/// Save a received file into a local `received_files/` directory.
fn save_received_file(filename: &str, data: &[u8]) {
    let dir = "received_files";
    if fs::create_dir_all(dir).is_err() {
        println!("*** Could not create {} directory", dir);
        return;
    }
    // Strip any path separators from the filename — never trust remote filenames
    let safe_name = Path::new(filename)
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("received_file");

    let path = format!("{}/{}", dir, safe_name);
    match fs::write(&path, data) {
        Ok(_)  => println!("\n*** File saved → {}", path),
        Err(e) => println!("\n*** Failed to save file: {}", e),
    }
}

// ── Actions ───────────────────────────────────────────────────────────────────
// We collect actions while holding the Mutex, then perform I/O AFTER releasing
// the lock. This avoids deadlocks and keeps lock-hold time short.
// (Same design principle as releasing a lock before a syscall in C.)

enum Action {
    NewPeer    { name: String },
    PrintText  { sender: String, text: String },
    FileStart  { filename: String, total_size: u64, total_chunks: u32 },
    FileDone   { filename: String, data: Vec<u8> },
    Forward    (Packet),
}

// ── Receive loop ──────────────────────────────────────────────────────────────

fn receive_loop(
    rx_socket:  UdpSocket,
    fwd_socket: UdpSocket,
    node:       Arc<Mutex<Node>>,
    port:       u16,
) {
    let mut buf = [0u8; RECV_BUF_SIZE];

    loop {
        // recvfrom blocks until a datagram arrives — this is fine because this
        // runs on its own dedicated thread, not blocking the main/stdin thread.
        let n = match rx_socket.recv_from(&mut buf) {
            Ok((n, _src)) => n,
            Err(_)        => break,
        };

        if let Some(packet) = Packet::from_bytes(&buf[..n]) {
            handle_packet(packet, &node, &fwd_socket, port);
        }
    }
}

fn handle_packet(
    packet: Packet,
    node:   &Arc<Mutex<Node>>,
    socket: &UdpSocket,
    port:   u16,
) {
    // ── Collect actions while holding the lock ────────────────────────────────
    let actions: Vec<Action> = {
        let mut n = node.lock().unwrap();

        // Drop packets we've already seen — prevents infinite relay loops.
        if !n.mark_seen(packet.origin_id, packet.packet_id) { return; }

        // Drop our own packets (we receive our own broadcasts on most systems).
        if packet.origin_id == n.device_id { return; }

        let my_id = n.device_id;
        let mut actions = Vec::new();

        // Process the content if this packet is addressed to us or is a broadcast.
        if packet.for_device(&my_id) {
            match packet.msg_type {
                MsgType::Announce => {
                    let name   = String::from_utf8_lossy(&packet.payload).into_owned();
                    let is_new = !n.neighbors.contains_key(&packet.origin_id);
                    n.update_neighbor(packet.origin_id, name.clone());
                    if is_new {
                        actions.push(Action::NewPeer { name });
                    }
                }

                MsgType::Text => {
                    let text   = String::from_utf8_lossy(&packet.payload).into_owned();
                    let sender = n.neighbors.get(&packet.origin_id)
                        .map(|nb| nb.name.clone())
                        .unwrap_or_else(|| short_id(&packet.origin_id));
                    actions.push(Action::PrintText { sender, text });
                }

                MsgType::FileInfo => {
                    if let Some(info) = FileInfoPayload::from_bytes(&packet.payload) {
                        actions.push(Action::FileStart {
                            filename:     info.filename.clone(),
                            total_size:   info.total_size,
                            total_chunks: info.total_chunks,
                        });
                        n.register_file(info.file_id, info.filename, info.total_chunks, info.total_size);
                    }
                }

                MsgType::FileChunk => {
                    if let Some(chunk) = FileChunkPayload::from_bytes(&packet.payload) {
                        if let Some((filename, data)) = n.add_chunk(
                            chunk.file_id, chunk.chunk_index, chunk.data
                        ) {
                            actions.push(Action::FileDone { filename, data });
                        }
                    }
                }
            }
        }

        // Always try to forward — flooding ensures all reachable nodes get the packet.
        // The dedup check above prevents infinite loops.
        if let Some(fwd) = packet.forwarded() {
            actions.push(Action::Forward(fwd));
        }

        actions
    }; // ← Mutex released here, before any I/O

    // ── Perform I/O without holding the lock ──────────────────────────────────
    for action in actions {
        match action {
            Action::NewPeer { name } =>
                println!("\n*** {} joined the network", name),

            Action::PrintText { sender, text } =>
                println!("\n[{}] {}", sender, text),

            Action::FileStart { filename, total_size, total_chunks } =>
                println!("\n*** Incoming: {} ({} bytes, {} chunks)", filename, total_size, total_chunks),

            Action::FileDone { filename, data } =>
                save_received_file(&filename, &data),

            Action::Forward(pkt) =>
                broadcast(socket, &pkt, port),
        }
    }
}

// ── File sending ──────────────────────────────────────────────────────────────

fn send_file(
    node:      &Arc<Mutex<Node>>,
    socket:    &UdpSocket,
    device_id: DeviceId,
    path:      &str,
    port:      u16,
) {
    let data = match fs::read(path) {
        Ok(d)  => d,
        Err(e) => { println!("Cannot read '{}': {}", path, e); return; }
    };

    let filename = Path::new(path)
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("file");

    let total_size   = data.len() as u64;
    let total_chunks = ((data.len() + CHUNK_DATA_SIZE - 1) / CHUNK_DATA_SIZE) as u32;
    let file_id: u32 = rand::random::<u32>();

    println!("Sending '{}' ({} bytes, {} chunks)...", filename, total_size, total_chunks);

    // Send FileInfo first so receivers know what's coming
    let packet_id = { let mut n = node.lock().unwrap(); n.next_packet_id() };
    let info_payload = make_file_info_payload(file_id, total_chunks, total_size, filename);
    let info_pkt = Packet::new(MsgType::FileInfo, device_id, BROADCAST_ID, packet_id, info_payload);
    broadcast(socket, &info_pkt, port);

    // Small pause so receivers can process FileInfo before chunks start arriving
    thread::sleep(Duration::from_millis(100));

    // Send chunks — each is a separate UDP datagram
    for (i, chunk) in data.chunks(CHUNK_DATA_SIZE).enumerate() {
        let packet_id = { let mut n = node.lock().unwrap(); n.next_packet_id() };
        let chunk_payload = make_file_chunk_payload(file_id, i as u32, chunk);
        let chunk_pkt = Packet::new(MsgType::FileChunk, device_id, BROADCAST_ID, packet_id, chunk_payload);
        broadcast(socket, &chunk_pkt, port);

        // Pacing: don't flood the network — give relays time to forward each chunk
        thread::sleep(Duration::from_millis(5));

        if (i + 1) % 20 == 0 || i + 1 == total_chunks as usize {
            println!("  {}/{} chunks sent", i + 1, total_chunks);
        }
    }

    println!("'{}' sent.", filename);
}

// ── Main ──────────────────────────────────────────────────────────────────────

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <your_name> [port]", args[0]);
        eprintln!("  e.g: {} Alice", args[0]);
        std::process::exit(1);
    }

    let name = args[1].clone();
    let port: u16 = args.get(2)
        .and_then(|p| p.parse().ok())
        .unwrap_or(RELAY_PORT);

    // Generate a random 16-byte device ID — unique per run.
    // In a real deployment you'd save this to disk so the ID persists across reboots.
    let device_id: DeviceId = std::array::from_fn(|_| rand::random::<u8>());
    let id_display = format!("{}...{}", &hex_id(&device_id)[..8], &hex_id(&device_id)[24..]);

    println!("╔══════════════════════════════════╗");
    println!("║         R U S T _ R E L A Y      ║");
    println!("╠══════════════════════════════════╣");
    println!("║ Name:  {:<25}║", name);
    println!("║ ID:    {:<25}║", id_display);
    println!("║ Port:  {:<25}║", port);
    println!("╠══════════════════════════════════╣");
    println!("║ /peers             list devices  ║");
    println!("║ /sendfile <path>   send a file   ║");
    println!("║ /quit              exit          ║");
    println!("║ <anything else>    broadcast msg ║");
    println!("╚══════════════════════════════════╝");
    println!();

    // ── Socket setup ──────────────────────────────────────────────────────────
    // Bind to 0.0.0.0 (all interfaces) on our port so we receive broadcasts.
    // set_broadcast(true) allows sending to 255.255.255.255.
    let bind_addr = format!("0.0.0.0:{}", port);
    let socket = UdpSocket::bind(&bind_addr)
        .unwrap_or_else(|e| { eprintln!("Bind failed: {} — is port {} in use?", e, port); std::process::exit(1); });
    socket.set_broadcast(true).expect("Failed to enable broadcast");

    // try_clone() gives each thread its own socket handle to the same OS socket.
    // This is safe — the OS handles concurrent access on the same file descriptor.
    let rx_socket  = socket.try_clone().expect("socket clone failed");
    let fwd_socket = socket.try_clone().expect("socket clone failed");
    let ann_socket = socket.try_clone().expect("socket clone failed");

    let node = Arc::new(Mutex::new(Node::new(device_id, name.clone())));

    // ── Receive thread ────────────────────────────────────────────────────────
    // Blocks on recv_from(), processes each arriving packet.
    {
        let node = Arc::clone(&node);
        thread::spawn(move || receive_loop(rx_socket, fwd_socket, node, port));
    }

    // ── Announce thread ───────────────────────────────────────────────────────
    // Periodically broadcast "I'm here" so neighbors can discover us.
    {
        let node = Arc::clone(&node);
        thread::spawn(move || {
            loop {
                let (my_id, my_name, packet_id) = {
                    let mut n = node.lock().unwrap();
                    (n.device_id, n.name.clone(), n.next_packet_id())
                };
                let payload = make_announce_payload(&my_name);
                let pkt = Packet::new(MsgType::Announce, my_id, BROADCAST_ID, packet_id, payload);
                broadcast(&ann_socket, &pkt, port);
                thread::sleep(ANNOUNCE_INTERVAL);
            }
        });
    }

    // ── Main thread: stdin CLI ────────────────────────────────────────────────
    // Reads lines from stdin. Blocks waiting for input — that's fine because
    // the receive thread handles incoming packets independently.
    let stdin = io::stdin();
    for line in stdin.lock().lines() {
        let line = match line {
            Ok(l) => l.trim().to_string(),
            Err(_) => break,
        };

        if line.is_empty() { continue; }

        match line.as_str() {
            "/quit" => {
                println!("Goodbye.");
                break;
            }
            "/peers" => {
                let n = node.lock().unwrap();
                if n.neighbors.is_empty() {
                    println!("No peers discovered yet. Waiting for announcements...");
                } else {
                    println!("Known peers ({}):", n.neighbors.len());
                    for (id, nb) in &n.neighbors {
                        println!("  {} (id: {}...)", nb.name, short_id(id));
                    }
                }
            }
            _ if line.starts_with("/sendfile ") => {
                let path = line["/sendfile ".len()..].trim();
                send_file(&node, &socket, device_id, path, port);
            }
            _ if line.starts_with('/') => {
                println!("Unknown command. Try /peers, /sendfile <path>, /quit");
            }
            _ => {
                // Broadcast a text message
                let packet_id = { let mut n = node.lock().unwrap(); n.next_packet_id() };
                let payload = make_text_payload(&line);
                let pkt = Packet::new(MsgType::Text, device_id, BROADCAST_ID, packet_id, payload);
                broadcast(&socket, &pkt, port);
                println!("[you] {}", line);
            }
        }
    }
}
