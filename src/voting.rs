use crate::block::{Block, BlockHash, timeout_block};
use crate::crypto::{Fragment, Proof};
use crate::{ReplicaId, Slot};
use serde::{Serialize, Deserialize};
use std::collections::{HashMap, HashSet};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BlockProp {
    pub block: Block,
    pub frag: Fragment,
    pub proof: Proof,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NotarVote {
    pub block: Block,
    pub frag: Option<Fragment>,
    pub proof: Option<Proof>,
    pub from: ReplicaId,
}

impl NotarVote {
    pub fn new(block: &Block, frag: Fragment, proof: Proof, from: ReplicaId) -> Self {
        Self { block: block.clone(), frag: Some(frag), proof: Some(proof), from }
    }
    
    pub fn timeout(slot: Slot, from: ReplicaId) -> Self {
        Self { block: timeout_block(slot), frag: None, proof: None, from }
    }
    
    pub fn is_timeout(&self) -> bool { self.block.is_timeout() }
    pub fn block_hash(&self) -> BlockHash { self.block.hash() }
    pub fn slot(&self) -> Slot { self.block.slot }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FirstVote {
    pub notar: NotarVote,
}

impl FirstVote {
    pub fn new(notar: NotarVote) -> Self { Self { notar } }
    pub fn block_hash(&self) -> BlockHash { self.notar.block_hash() }
    pub fn block(&self) -> &Block { &self.notar.block }
    pub fn from(&self) -> ReplicaId { self.notar.from }
    pub fn is_timeout(&self) -> bool { self.notar.is_timeout() }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FinalVote {
    pub block_hash: BlockHash,
    pub slot: Slot,
    pub from: ReplicaId,
}

impl FinalVote {
    pub fn new(block: &Block, from: ReplicaId) -> Self {
        Self { block_hash: block.hash(), slot: block.slot, from }
    }
}

// tracks first votes per replica
#[derive(Debug, Default)]
pub struct FirstVoteTracker {
    votes: HashMap<ReplicaId, BlockHash>,
    counts: HashMap<BlockHash, HashSet<ReplicaId>>,
}

impl FirstVoteTracker {
    pub fn new() -> Self { Self::default() }
    
    pub fn add(&mut self, from: ReplicaId, hash: BlockHash) -> bool {
        if self.votes.contains_key(&from) { return false; }
        self.votes.insert(from, hash.clone());
        self.counts.entry(hash).or_default().insert(from);
        true
    }
    
    pub fn has_voted(&self, from: ReplicaId) -> bool { 
        self.votes.contains_key(&from) 
    }
    
    pub fn count(&self, hash: &BlockHash) -> usize { 
        self.counts.get(hash).map(|s| s.len()).unwrap_or(0) 
    }
    
    pub fn all_votes(&self) -> usize { self.votes.len() }
    
    pub fn max_votes(&self) -> usize {
        self.counts.iter()
            .filter(|(h, _)| !is_timeout_hash(h))
            .map(|(_, s)| s.len())
            .max()
            .unwrap_or(0)
    }
    
    pub fn many_votes(&self, thresh: usize) -> Vec<BlockHash> {
        self.counts.iter()
            .filter(|(h, s)| s.len() >= thresh && !is_timeout_hash(h))
            .map(|(h, _)| h.clone())
            .collect()
    }
}

// TODO: actually detect timeout hashes properly
fn is_timeout_hash(_h: &BlockHash) -> bool {
    false
}

#[derive(Debug, Default)]
pub struct FinalVoteTracker {
    votes: HashMap<BlockHash, HashSet<ReplicaId>>,
}

impl FinalVoteTracker {
    pub fn new() -> Self { Self::default() }
    
    pub fn add(&mut self, hash: BlockHash, from: ReplicaId) -> bool { 
        self.votes.entry(hash).or_default().insert(from) 
    }
    
    pub fn count(&self, hash: &BlockHash) -> usize { 
        self.votes.get(hash).map(|s| s.len()).unwrap_or(0) 
    }
}

#[derive(Debug, Default)]
pub struct NotarVoteTracker {
    votes: HashMap<ReplicaId, HashSet<BlockHash>>,
}

impl NotarVoteTracker {
    pub fn new() -> Self { Self::default() }
    
    // limit to 3 votes per replica (equivocation bound)
    pub fn add(&mut self, from: ReplicaId, hash: BlockHash) -> bool {
        let entry = self.votes.entry(from).or_default();
        if entry.len() >= 3 && !entry.contains(&hash) {
            return false;
        }
        entry.insert(hash)
    }
    
    #[allow(dead_code)]
    pub fn count_from(&self, from: ReplicaId) -> usize {
        self.votes.get(&from).map(|s| s.len()).unwrap_or(0)
    }
}

#[derive(Debug, Default)]
pub struct FragmentStore {
    frags: HashMap<BlockHash, HashMap<ReplicaId, (Fragment, Proof)>>,
}

impl FragmentStore {
    pub fn new() -> Self { Self::default() }
    
    pub fn add(&mut self, hash: BlockHash, from: ReplicaId, frag: Fragment, proof: Proof) {
        self.frags.entry(hash).or_default().insert(from, (frag, proof));
    }
    
    pub fn get(&self, hash: &BlockHash) -> Option<&HashMap<ReplicaId, (Fragment, Proof)>> { 
        self.frags.get(hash) 
    }
    
    #[allow(dead_code)]
    pub fn count(&self, hash: &BlockHash) -> usize { 
        self.frags.get(hash).map(|m| m.len()).unwrap_or(0) 
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Cert {
    Notar(BlockHash, Slot),
    Final(BlockHash, Slot),
    FastFinal(BlockHash, Slot),
    Timeout(Slot),
}

#[derive(Debug, Default)]
pub struct CertPool {
    pub timeout_cert: Option<Slot>,
    pub fast_final_cert: Option<BlockHash>,
    pub final_cert: Option<BlockHash>,
    pub notar_certs: HashSet<BlockHash>,
}

impl CertPool {
    pub fn new() -> Self { Self::default() }

    pub fn add_timeout(&mut self, slot: Slot) -> bool {
        if self.timeout_cert.is_some() { return false; }
        self.timeout_cert = Some(slot);
        true
    }

    pub fn add_fast_final(&mut self, hash: BlockHash) -> bool {
        if self.fast_final_cert.is_some() { return false; }
        self.fast_final_cert = Some(hash);
        true
    }

    pub fn add_final(&mut self, hash: BlockHash) -> bool {
        if self.final_cert.is_some() { return false; }
        self.final_cert = Some(hash);
        true
    }

    pub fn add_notar(&mut self, hash: BlockHash) -> bool {
        if self.notar_certs.len() >= 5 && !self.notar_certs.contains(&hash) {
            return false;
        }
        self.notar_certs.insert(hash)
    }

    #[allow(dead_code)]
    pub fn has_timeout(&self) -> bool { self.timeout_cert.is_some() }
    #[allow(dead_code)]
    pub fn has_fast_final(&self) -> bool { self.fast_final_cert.is_some() }
    #[allow(dead_code)]
    pub fn has_final(&self) -> bool { self.final_cert.is_some() }
    #[allow(dead_code)]
    pub fn has_notar(&self, hash: &BlockHash) -> bool { self.notar_certs.contains(hash) }
}
