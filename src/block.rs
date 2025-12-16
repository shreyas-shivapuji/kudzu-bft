use crate::Slot;
use sha2::{Sha256, Digest};
use serde::{Serialize, Deserialize};

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BlockHash(pub [u8; 32]);

impl BlockHash {
    pub fn zero() -> Self { Self([0u8; 32]) }
    pub fn from_bytes(b: [u8; 32]) -> Self { Self(b) }
}

impl std::fmt::Display for BlockHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", hex::encode(&self.0[..8]))
    }
}

// tag for erasure coded data
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Tag {
    pub len: usize,   // original data length
    pub root: [u8; 32], // merkle root
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Block {
    pub slot: Slot,
    pub tag: Tag,
    pub parent_hash: BlockHash,
}

impl Block {
    pub fn new(slot: Slot, tag: Tag, parent: BlockHash) -> Self {
        Self { slot, tag, parent_hash: parent }
    }

    pub fn hash(&self) -> BlockHash {
        let mut h = Sha256::new();
        h.update(self.slot.0.to_le_bytes());
        h.update(self.tag.len.to_le_bytes());
        h.update(&self.tag.root);
        h.update(&self.parent_hash.0);
        BlockHash(h.finalize().into())
    }

    pub fn is_timeout(&self) -> bool {
        self.tag.len == 0 && self.tag.root == [0u8; 32] && self.parent_hash == BlockHash::zero()
    }
}

// special block for when slot times out
pub fn timeout_block(slot: Slot) -> Block {
    Block {
        slot,
        tag: Tag { len: 0, root: [0u8; 32] },
        parent_hash: BlockHash::zero(),
    }
}

pub fn genesis() -> Block {
    Block {
        slot: Slot::new(0),
        tag: Tag { len: 0, root: [0u8; 32] },
        parent_hash: BlockHash::zero(),
    }
}
