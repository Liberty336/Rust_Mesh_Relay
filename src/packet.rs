//! packet.rs — wire format for Rust_Relay.
//!
//! Every frame on the network looks like:
//!
//!  ┌────────┬─────────┬───────────┬──────────┬───────────┬──────────┬──────────┬─────────────┬──────────┬─────────────┐
//!  │ magic  │ version │ msg_type  │ origin   │ dest      │ pkt_id   │ hops/max │ payload_len │ checksum │ payload     │
//!  │ 4 B    │ 1 B     │ 1 B       │ 16 B     │ 16 B      │ 4 B      │ 1 B + 1B │ 2 B         │ 4 B      │ variable    │
//!  └────────┴─────────┴───────────┴──────────┴───────────┴──────────┴──────────┴─────────────┴──────────┴─────────────┘
//!  Total header: 50 bytes. Fits comfortably in a UDP datagram.
//!
//! Key fields:
//!  - origin_id:  the device that CREATED the packet (never changes during forwarding)
//!  - dest_id:    FF FF ... FF = broadcast to everyone
//!  - packet_id:  unique per (origin, packet) — used to deduplicate forwards
//!  - hop_count:  incremented by each relay. Dropped when == max_hops.

// ── Constants ─────────────────────────────────────────────────────────────────

pub const MAGIC:           [u8; 4] = *b"RLAY";
pub const VERSION:         u8      = 1;
pub const MAX_HOPS:        u8      = 8;    // max times a packet can be relayed
pub const BROADCAST_ID:    [u8; 16]= [0xFF; 16];
pub const HEADER_SIZE:     usize   = 50;
pub const RELAY_PORT:      u16     = 37760;
pub const CHUNK_DATA_SIZE: usize   = 1024; // bytes of file data per chunk packet

pub type DeviceId = [u8; 16];

// ── Message types ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum MsgType {
    Announce  = 0x01, // "I'm here" — broadcast periodically
    Text      = 0x02, // a text message
    FileInfo  = 0x03, // metadata about an incoming file transfer
    FileChunk = 0x04, // one chunk of a file transfer
}

impl MsgType {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0x01 => Some(Self::Announce),
            0x02 => Some(Self::Text),
            0x03 => Some(Self::FileInfo),
            0x04 => Some(Self::FileChunk),
            _    => None,
        }
    }
}

// ── Packet ────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct Packet {
    pub version:   u8,
    pub msg_type:  MsgType,
    pub origin_id: DeviceId,
    pub dest_id:   DeviceId,
    pub packet_id: u32,
    pub hop_count: u8,
    pub max_hops:  u8,
    pub payload:   Vec<u8>,
}

impl Packet {
    pub fn new(
        msg_type:  MsgType,
        origin_id: DeviceId,
        dest_id:   DeviceId,
        packet_id: u32,
        payload:   Vec<u8>,
    ) -> Self {
        Self {
            version: VERSION,
            msg_type,
            origin_id,
            dest_id,
            packet_id,
            hop_count: 0,
            max_hops: MAX_HOPS,
            payload,
        }
    }

    pub fn is_broadcast(&self) -> bool {
        self.dest_id == BROADCAST_ID
    }

    /// True if this packet is addressed to `id` or is a broadcast.
    pub fn for_device(&self, id: &DeviceId) -> bool {
        self.is_broadcast() || &self.dest_id == id
    }

    /// Returns a clone with hop_count incremented, or None if max_hops reached.
    /// Called by each relay before re-transmitting.
    pub fn forwarded(&self) -> Option<Self> {
        if self.hop_count >= self.max_hops {
            return None; // packet's TTL expired — drop it
        }
        let mut copy = self.clone();
        copy.hop_count += 1;
        Some(copy)
    }

    /// Serialize to bytes for transmission over UDP.
    pub fn to_bytes(&self) -> Vec<u8> {
        let payload_len = self.payload.len() as u16;
        let checksum: u32 = self.payload.iter().map(|&b| b as u32).sum();

        let mut buf = Vec::with_capacity(HEADER_SIZE + self.payload.len());
        buf.extend_from_slice(&MAGIC);
        buf.push(self.version);
        buf.push(self.msg_type as u8);
        buf.extend_from_slice(&self.origin_id);
        buf.extend_from_slice(&self.dest_id);
        buf.extend_from_slice(&self.packet_id.to_be_bytes());
        buf.push(self.hop_count);
        buf.push(self.max_hops);
        buf.extend_from_slice(&payload_len.to_be_bytes());
        buf.extend_from_slice(&checksum.to_be_bytes());
        buf.extend_from_slice(&self.payload);
        buf
    }

    /// Deserialize from a received UDP datagram.
    /// Returns None for malformed packets or checksum failures — no panics.
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < HEADER_SIZE { return None; }
        if data[0..4] != MAGIC      { return None; }

        let version  = data[4];
        let msg_type = MsgType::from_u8(data[5])?;

        let mut origin_id = [0u8; 16];
        origin_id.copy_from_slice(&data[6..22]);
        let mut dest_id = [0u8; 16];
        dest_id.copy_from_slice(&data[22..38]);

        let packet_id   = u32::from_be_bytes(data[38..42].try_into().ok()?);
        let hop_count   = data[42];
        let max_hops    = data[43];
        let payload_len = u16::from_be_bytes(data[44..46].try_into().ok()?) as usize;
        let checksum    = u32::from_be_bytes(data[46..50].try_into().ok()?);

        if data.len() < HEADER_SIZE + payload_len { return None; }
        let payload = data[HEADER_SIZE..HEADER_SIZE + payload_len].to_vec();

        // Verify checksum — same additive approach as the original protocol
        let expected: u32 = payload.iter().map(|&b| b as u32).sum();
        if expected != checksum { return None; }

        Some(Self { version, msg_type, origin_id, dest_id, packet_id, hop_count, max_hops, payload })
    }
}

// ── Payload builders & parsers ────────────────────────────────────────────────
// Each message type has a different payload layout. These functions handle
// the encoding/decoding so main.rs stays clean.

pub fn make_announce_payload(name: &str) -> Vec<u8> {
    name.as_bytes().to_vec()
}

pub fn make_text_payload(text: &str) -> Vec<u8> {
    text.as_bytes().to_vec()
}

/// FileInfo payload layout:
///   file_id:      u32  (4 bytes) — random ID shared by all chunks of this file
///   total_chunks: u32  (4 bytes)
///   total_size:   u64  (8 bytes) — original file size in bytes
///   filename:     UTF-8 bytes (rest) — basename only, no path
pub fn make_file_info_payload(file_id: u32, total_chunks: u32, total_size: u64, filename: &str) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&file_id.to_be_bytes());
    buf.extend_from_slice(&total_chunks.to_be_bytes());
    buf.extend_from_slice(&total_size.to_be_bytes());
    buf.extend_from_slice(filename.as_bytes());
    buf
}

/// FileChunk payload layout:
///   file_id:     u32  (4 bytes) — matches the FileInfo file_id
///   chunk_index: u32  (4 bytes) — zero-based chunk number
///   data:        bytes (rest)   — raw chunk data
pub fn make_file_chunk_payload(file_id: u32, chunk_index: u32, data: &[u8]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&file_id.to_be_bytes());
    buf.extend_from_slice(&chunk_index.to_be_bytes());
    buf.extend_from_slice(data);
    buf
}

pub struct FileInfoPayload {
    pub file_id:      u32,
    pub total_chunks: u32,
    pub total_size:   u64,
    pub filename:     String,
}

impl FileInfoPayload {
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < 16 { return None; }
        Some(Self {
            file_id:      u32::from_be_bytes(data[0..4].try_into().ok()?),
            total_chunks: u32::from_be_bytes(data[4..8].try_into().ok()?),
            total_size:   u64::from_be_bytes(data[8..16].try_into().ok()?),
            filename:     String::from_utf8_lossy(&data[16..]).into_owned(),
        })
    }
}

pub struct FileChunkPayload {
    pub file_id:     u32,
    pub chunk_index: u32,
    pub data:        Vec<u8>,
}

impl FileChunkPayload {
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < 8 { return None; }
        Some(Self {
            file_id:     u32::from_be_bytes(data[0..4].try_into().ok()?),
            chunk_index: u32::from_be_bytes(data[4..8].try_into().ok()?),
            data:        data[8..].to_vec(),
        })
    }
}
