//! node.rs — shared state for a Rust_Relay node.
//!
//! This struct lives inside an Arc<Mutex<Node>> shared between threads:
//!   - the receive thread reads/writes it when packets arrive
//!   - the announce thread reads it to get our name
//!   - the main thread reads it for /peers and writes it when sending
//!
//! Arc = shared ownership across threads (reference counted)
//! Mutex = only one thread can access the data at a time

use std::collections::{HashMap, HashSet};
use std::time::Instant;

use crate::packet::DeviceId;

// ── Neighbor ──────────────────────────────────────────────────────────────────

pub struct Neighbor {
    pub name:      String,
    pub last_seen: Instant, // for pruning stale peers
}

// ── File reassembly ───────────────────────────────────────────────────────────
// File transfer works like a jigsaw puzzle:
//   1. We receive a FileInfo packet telling us how many pieces to expect
//   2. FileChunk packets arrive (possibly out of order — UDP has no ordering)
//   3. When chunks.len() == total_chunks, we have the whole file

pub struct FileReassembly {
    pub filename:     String,
    pub total_chunks: u32,
    pub total_size:   u64,
    pub chunks:       HashMap<u32, Vec<u8>>, // chunk_index → data
}

impl FileReassembly {
    pub fn is_complete(&self) -> bool {
        self.chunks.len() as u32 == self.total_chunks
    }

    /// Reassemble chunks in order into the complete file bytes.
    pub fn reassemble(&self) -> Vec<u8> {
        let mut data = Vec::with_capacity(self.total_size as usize);
        for i in 0..self.total_chunks {
            if let Some(chunk) = self.chunks.get(&i) {
                data.extend_from_slice(chunk);
            }
        }
        data
    }
}

// ── Node ──────────────────────────────────────────────────────────────────────

pub struct Node {
    pub device_id:      DeviceId,
    pub name:           String,
    packet_counter:     u32,
    pub neighbors:      HashMap<DeviceId, Neighbor>,
    seen:               HashSet<(DeviceId, u32)>, // (origin_id, packet_id) pairs
    pub incoming_files: HashMap<u32, FileReassembly>, // file_id → partial file
}

impl Node {
    pub fn new(device_id: DeviceId, name: String) -> Self {
        Self {
            device_id,
            name,
            packet_counter: 0,
            neighbors: HashMap::new(),
            seen: HashSet::new(),
            incoming_files: HashMap::new(),
        }
    }

    /// Get the next unique packet ID for outbound packets.
    /// wrapping_add() prevents overflow panic — rolls over to 0 after u32::MAX.
    pub fn next_packet_id(&mut self) -> u32 {
        self.packet_counter = self.packet_counter.wrapping_add(1);
        self.packet_counter
    }

    /// Returns true if this is the first time we've seen this (origin, packet_id).
    /// Used to prevent forwarding the same packet twice (deduplication).
    pub fn mark_seen(&mut self, origin: DeviceId, packet_id: u32) -> bool {
        let key = (origin, packet_id);
        if self.seen.contains(&key) {
            return false; // duplicate — already forwarded this
        }
        // Simple size cap: if we've accumulated too many entries, clear old ones.
        // A production version would use a ring buffer keyed by timestamp.
        if self.seen.len() > 2000 {
            self.seen.clear();
        }
        self.seen.insert(key);
        true // new packet
    }

    pub fn update_neighbor(&mut self, id: DeviceId, name: String) {
        self.neighbors.insert(id, Neighbor { name, last_seen: Instant::now() });
    }

    /// Register incoming file metadata (from a FileInfo packet).
    pub fn register_file(&mut self, file_id: u32, filename: String, total_chunks: u32, total_size: u64) {
        self.incoming_files.insert(file_id, FileReassembly {
            filename,
            total_chunks,
            total_size,
            chunks: HashMap::new(),
        });
    }

    /// Add a received chunk. Returns Some((filename, data)) if the file is now complete.
    pub fn add_chunk(&mut self, file_id: u32, chunk_index: u32, data: Vec<u8>) -> Option<(String, Vec<u8>)> {
        let reassembly = self.incoming_files.get_mut(&file_id)?;
        reassembly.chunks.insert(chunk_index, data);

        if reassembly.is_complete() {
            // Remove from the map and return the completed file
            let done = self.incoming_files.remove(&file_id)?;
            Some((done.filename, done.reassemble()))
        } else {
            None // still waiting for more chunks
        }
    }
}
