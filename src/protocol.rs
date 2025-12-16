use crate::block::{Block, BlockHash, Tag, genesis, timeout_block};
use crate::crypto::{self, Fragment};
use crate::types::leader_for_slot;
use crate::voting::*;
use crate::{ReplicaId, Slot};
use std::collections::{HashMap, HashSet};

pub const DELTA_TIMEOUT: u64 = 5000; // ms

pub struct SlotState {
    pub t_start: u64,
    pub done: bool,
    pub proposed: bool,
    pub first_voted: bool,
    pub notarized: HashSet<BlockHash>,
    pub second_look: HashSet<BlockHash>,  // blocks we've tried to reconstruct
    pub first_votes: FirstVoteTracker,
    pub final_votes: FinalVoteTracker,
    pub notar_votes: NotarVoteTracker,
    pub cert_pool: CertPool,
    pub fragments: FragmentStore,
    pub blocks: HashMap<BlockHash, Block>,
}

impl SlotState {
    pub fn new(t: u64) -> Self {
        Self {
            t_start: t, done: false, proposed: false, first_voted: false,
            notarized: HashSet::new(), second_look: HashSet::new(),
            first_votes: FirstVoteTracker::new(), final_votes: FinalVoteTracker::new(),
            notar_votes: NotarVoteTracker::new(), cert_pool: CertPool::new(),
            fragments: FragmentStore::new(), blocks: HashMap::new(),
        }
    }
}

#[derive(Clone, Debug)]
pub enum Action {
    Send(ReplicaId, BlockProp),
    BcastFirst(FirstVote),
    BcastFinal(FinalVote),
    BcastNotar(NotarVote),
    Done(BlockHash),
}

pub struct Replica {
    pub id: ReplicaId,
    pub n: usize,
    pub f: usize,
    pub p: usize,
    pub slot: Slot,
    pub state: SlotState,
    pub parent_block: BlockHash,
    pub finalized_blocks: Vec<Block>,
    pub time: u64,
    pub block_tree: HashMap<BlockHash, Block>,
}

impl Replica {
    pub fn new(id: ReplicaId, n: usize, f: usize, p: usize) -> Self {
        assert!(n >= 3 * f + 2 * p + 1);
        let gen = genesis();
        let gen_hash = gen.hash();
        let mut block_tree = HashMap::new();
        block_tree.insert(gen_hash.clone(), gen);
        Self { 
            id, n, f, p, 
            slot: Slot::new(1), 
            state: SlotState::new(0), 
            parent_block: gen_hash, 
            finalized_blocks: Vec::new(), 
            time: 0, 
            block_tree 
        }
    }

    pub fn quorum(&self) -> usize { self.n - self.f - self.p }
    pub fn fast_quorum(&self) -> usize { self.n - self.p }
    pub fn k(&self) -> usize { self.f + self.p + 1 }  // reconstruction threshold
    pub fn leader(&self, slot: Slot) -> ReplicaId { leader_for_slot(slot, self.n) }
    pub fn is_leader(&self) -> bool { self.leader(self.slot) == self.id }

    // got a block in our tree - finalize it
    pub fn on_block_in_block_tree(&mut self, block: Block) -> Vec<Action> {
        let mut acts = Vec::new();
        if block.slot != self.slot || self.state.done { 
            return acts; 
        }
        let hash = block.hash();
        self.parent_block = hash.clone();
        self.state.done = true;
        // if we only notarized this one block (or nothing), send final vote
        if self.state.notarized.len() <= 1 && (self.state.notarized.is_empty() || self.state.notarized.contains(&hash)) {
            acts.push(Action::BcastFinal(FinalVote::new(&block, self.id)));
        }
        self.block_tree.insert(hash.clone(), block);
        acts.push(Action::Done(hash));
        acts
    }

    pub fn on_timeout_cert(&mut self) -> Vec<Action> {
        if self.state.done { return Vec::new(); }
        if !self.state.cert_pool.add_timeout(self.slot) { return Vec::new(); }
        self.state.done = true;
        Vec::new()
    }

    // leader creates and sends proposal
    pub fn propose(&mut self, payload: &[u8]) -> Result<Vec<Action>, String> {
        if !self.is_leader() { return Err("not leader".into()); }
        if self.state.proposed { return Err("already proposed".into()); }
        self.state.proposed = true;
        self.state.first_voted = true;  // implicit vote for own block
        
        let k = self.k();
        let frags = crypto::encode(payload, self.n, k)?;
        let (root, proofs) = crypto::merkle_tree(&frags);
        let tag = Tag { len: payload.len(), root };
        let block = Block::new(self.slot, tag, self.parent_block.clone());
        let hash = block.hash();
        
        self.state.blocks.insert(hash.clone(), block.clone());
        self.block_tree.insert(hash.clone(), block.clone());
        self.state.first_votes.add(self.id, hash.clone());  // count own vote
        self.state.notarized.insert(hash.clone());
        self.state.fragments.add(hash.clone(), self.id, frags[self.id].clone(), proofs[self.id].clone());
        
        let mut acts = Vec::new();
        // send different fragment to each node
        for (i, (frag, proof)) in frags.iter().zip(proofs.iter()).enumerate() {
            acts.push(Action::Send(i, BlockProp { block: block.clone(), frag: frag.clone(), proof: proof.clone() }));
        }
        // broadcast our vote too
        acts.push(Action::BcastFirst(FirstVote::new(NotarVote::new(
            &block, frags[self.id].clone(), proofs[self.id].clone(), self.id
        ))));
        Ok(acts)
    }

    // received proposal from leader
    pub fn on_proposal(&mut self, prop: BlockProp, from: ReplicaId) -> Vec<Action> {
        let mut acts = Vec::new();
        if self.state.first_voted || prop.block.slot != self.slot { return acts; }
        if from != self.leader(self.slot) { return acts; }
        // verify merkle proof
        if !crypto::verify_proof(&prop.block.tag.root, &prop.frag, &prop.proof) { return acts; }
        
        let hash = prop.block.hash();
        self.state.first_voted = true;
        self.state.blocks.insert(hash.clone(), prop.block.clone());
        self.state.fragments.add(hash.clone(), self.id, prop.frag.clone(), prop.proof.clone());
        self.state.first_votes.add(self.id, hash.clone());
        acts.push(Action::BcastFirst(FirstVote::new(NotarVote::new(&prop.block, prop.frag, prop.proof, self.id))));
        self.state.notarized.insert(hash);
        acts
    }

    // no proposal received, vote for timeout
    pub fn on_timeout(&mut self) -> Vec<Action> {
        let mut acts = Vec::new();
        if self.state.first_voted { return acts; }
        self.state.first_voted = true;
        let tb = timeout_block(self.slot);
        let th = tb.hash();
        self.state.first_votes.add(self.id, th.clone());
        acts.push(Action::BcastFirst(FirstVote::new(NotarVote::timeout(self.slot, self.id))));
        self.state.notarized.insert(th);
        acts
    }

    pub fn on_first_vote(&mut self, vote: &FirstVote, from: ReplicaId) -> Vec<Action> {
        let mut acts = Vec::new();
        if vote.notar.slot() != self.slot || self.state.first_votes.has_voted(from) { return acts; }
        let hash = vote.block_hash();
        
        if !vote.is_timeout() && !self.state.notar_votes.add(from, hash.clone()) {
            return acts;
        }
        
        if !vote.is_timeout() {
            self.state.blocks.entry(hash.clone()).or_insert_with(|| vote.block().clone());
        }
        
        self.state.first_votes.add(from, hash.clone());
        
        // store fragment if present
        if let (Some(frag), Some(proof)) = (&vote.notar.frag, &vote.notar.proof) {
            self.state.fragments.add(hash.clone(), from, frag.clone(), proof.clone());
        }
        
        acts.extend(self.check_enough_votes(&hash));
        acts.extend(self.check_timeout_votes());
        acts
    }

    // check if we have enough votes to try reconstruction
    fn check_enough_votes(&mut self, hash: &BlockHash) -> Vec<Action> {
        if !self.state.first_voted { return Vec::new(); }
        if self.state.second_look.contains(hash) { return Vec::new(); }
        if self.state.first_votes.count(hash) < self.k() { return Vec::new(); }
        
        // make sure parent is in our tree
        let parent_ok = if let Some(block) = self.state.blocks.get(hash) {
            self.block_tree.contains_key(&block.parent_hash)
        } else {
            false
        };
        if !parent_ok { return Vec::new(); }
        
        self.state.second_look.insert(hash.clone());
        self.try_reconstruct_and_notarize(hash)
    }

    // check if we should vote timeout (too many votes spread around)
    fn check_timeout_votes(&mut self) -> Vec<Action> {
        let mut acts = Vec::new();
        if !self.state.first_voted { return acts; }
        let all = self.state.first_votes.all_votes();
        let max = self.state.first_votes.max_votes();
        // if votes are too spread out, timeout is likely
        if all.saturating_sub(max) < self.k() { return acts; }
        let tb = timeout_block(self.slot);
        let th = tb.hash();
        if self.state.notarized.contains(&th) { return acts; }
        acts.push(Action::BcastNotar(NotarVote::timeout(self.slot, self.id)));
        self.state.notarized.insert(th);
        acts
    }

    pub fn try_reconstruct_and_notarize(&mut self, hash: &BlockHash) -> Vec<Action> {
        let mut acts = Vec::new();
        
        let payload = match self.try_reconstruct(hash) {
            Some(p) => p,
            None => {
                return self.send_timeout_vote();
            }
        };
        
        if let Some(block) = self.state.blocks.get(hash).cloned() {
            self.state.cert_pool.add_notar(hash.clone());
            self.block_tree.insert(hash.clone(), block.clone());
            
            if !self.state.notarized.contains(hash) {
                // get or create our fragment
                let (my_frag, my_proof) = if let Some(frags) = self.state.fragments.get(hash) {
                    if let Some((f, p)) = frags.get(&self.id) {
                        (f.clone(), p.clone())
                    } else {
                        // re-encode to get our fragment
                        match crypto::encode(&payload, self.n, self.k()) {
                            Ok(all_frags) => {
                                let (_, all_proofs) = crypto::merkle_tree(&all_frags);
                                (all_frags[self.id].clone(), all_proofs[self.id].clone())
                            }
                            Err(_) => return self.send_timeout_vote(),
                        }
                    }
                } else {
                    match crypto::encode(&payload, self.n, self.k()) {
                        Ok(all_frags) => {
                            let (_, all_proofs) = crypto::merkle_tree(&all_frags);
                            (all_frags[self.id].clone(), all_proofs[self.id].clone())
                        }
                        Err(_) => return self.send_timeout_vote(),
                    }
                };
                
                acts.push(Action::BcastNotar(NotarVote::new(&block, my_frag, my_proof, self.id)));
                self.state.notarized.insert(hash.clone());
            }
        }
        acts
    }

    fn send_timeout_vote(&mut self) -> Vec<Action> {
        let mut acts = Vec::new();
        let tb = timeout_block(self.slot);
        let th = tb.hash();
        if !self.state.notarized.contains(&th) {
            acts.push(Action::BcastNotar(NotarVote::timeout(self.slot, self.id)));
            self.state.notarized.insert(th);
        }
        acts
    }

    fn try_reconstruct(&self, hash: &BlockHash) -> Option<Vec<u8>> {
        let block = self.state.blocks.get(hash)?;
        let frags = self.state.fragments.get(hash)?;
        if frags.len() < self.k() { return None; }
        
        let mut arr: Vec<Option<Fragment>> = vec![None; self.n];
        for (rid, (frag, proof)) in frags {
            if crypto::verify_proof(&block.tag.root, frag, proof) {
                arr[*rid] = Some(frag.clone());
            }
        }
        crypto::decode(&arr, block.tag.len, self.n, self.k()).ok()
    }

    pub fn on_final_vote(&mut self, vote: &FinalVote) -> Option<Cert> {
        if vote.slot != self.slot { return None; }
        self.state.final_votes.add(vote.block_hash.clone(), vote.from);
        if self.state.final_votes.count(&vote.block_hash) >= self.quorum() {
            if self.state.cert_pool.add_final(vote.block_hash.clone()) {
                return Some(Cert::Final(vote.block_hash.clone(), vote.slot));
            }
        }
        None
    }

    pub fn check_fast_final(&mut self, hash: &BlockHash) -> Option<Cert> {
        if self.state.first_votes.count(hash) >= self.fast_quorum() {
            if self.state.cert_pool.add_fast_final(hash.clone()) {
                return Some(Cert::FastFinal(hash.clone(), self.slot));
            }
        }
        None
    }

    pub fn check_notar_cert(&mut self, hash: &BlockHash) -> Option<Cert> {
        if self.state.first_votes.count(hash) >= self.quorum() {
            if self.state.cert_pool.add_notar(hash.clone()) {
                return Some(Cert::Notar(hash.clone(), self.slot));
            }
        }
        None
    }

    pub fn check_timeout_cert(&mut self) -> Option<Cert> {
        let tb = timeout_block(self.slot);
        let th = tb.hash();
        if self.state.first_votes.count(&th) >= self.quorum() {
            if self.state.cert_pool.add_timeout(self.slot) {
                return Some(Cert::Timeout(self.slot));
            }
        }
        None
    }

    // finalize block and all its ancestors
    pub fn finalize_with_predecessors(&mut self, hash: &BlockHash) -> Vec<Block> {
        let mut chain = Vec::new();
        let mut current = hash.clone();
        
        // walk back to find all unfinalized ancestors
        while let Some(block) = self.block_tree.get(&current) {
            if self.finalized_blocks.iter().any(|b| b.hash() == current) {
                break;
            }
            chain.push(block.clone());
            if block.slot.0 == 0 { break; }  // hit genesis
            current = block.parent_hash.clone();
        }
        
        chain.reverse();
        for block in &chain {
            if !self.finalized_blocks.iter().any(|b| b.hash() == block.hash()) {
                self.finalized_blocks.push(block.clone());
            }
        }
        chain
    }

    pub fn advance_slot(&mut self, t: u64) {
        self.slot = self.slot.next();
        self.state = SlotState::new(t);
        self.time = t;
    }

    pub fn tick(&mut self, t: u64) -> Vec<Action> {
        self.time = t;
        // timeout if we haven't voted and time expired
        if !self.state.first_voted && t > self.state.t_start + DELTA_TIMEOUT {
            return self.on_timeout();
        }
        Vec::new()
    }
}
