use sha2::{Sha256, Digest};
use serde::{Serialize, Deserialize};

pub type ReplicaId = usize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Slot(pub u64);

impl Slot {
    pub fn new(v: u64) -> Self { Self(v) }
    pub fn next(&self) -> Self { Self(self.0 + 1) }
}

// leader election using hash
pub fn leader_for_slot(slot: Slot, n: usize) -> ReplicaId {
    let mut h = Sha256::new();
    h.update(b"KUDZU_LEADER");
    h.update(slot.0.to_le_bytes());
    let hash = h.finalize();
    let val = u64::from_le_bytes(hash[0..8].try_into().unwrap());
    (val as usize) % n
}
