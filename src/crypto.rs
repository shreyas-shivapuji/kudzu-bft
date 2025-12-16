use reed_solomon_erasure::galois_8::ReedSolomon;
use rs_merkle::{algorithms::Sha256 as MerkleSha256, Hasher, MerkleProof as RsProof, MerkleTree};
use serde::{Serialize, Deserialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Fragment(pub Vec<u8>);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Proof {
    pub hashes: Vec<[u8; 32]>,
    pub idx: usize,
    pub leaves: usize,
}

fn hash_frag(f: &Fragment) -> [u8; 32] {
    MerkleSha256::hash(&f.0)
}

// builds merkle tree from fragments
pub fn merkle_tree(frags: &[Fragment]) -> ([u8; 32], Vec<Proof>) {
    let n = frags.len();
    if n == 0 { return ([0u8; 32], vec![]); }

    let leaves: Vec<[u8; 32]> = frags.iter().map(hash_frag).collect();
    let tree = MerkleTree::<MerkleSha256>::from_leaves(&leaves);
    let root = tree.root().unwrap_or([0u8; 32]);

    let proofs = (0..n).map(|i| {
        let p = tree.proof(&[i]);
        Proof { hashes: p.proof_hashes().to_vec(), idx: i, leaves: n }
    }).collect();

    (root, proofs)
}

pub fn verify_proof(root: &[u8; 32], frag: &Fragment, proof: &Proof) -> bool {
    let leaf = hash_frag(frag);
    let p = RsProof::<MerkleSha256>::new(proof.hashes.clone());
    p.verify(*root, &[proof.idx], &[leaf], proof.leaves)
}

// reed-solomon encode data into n fragments
pub fn encode(data: &[u8], n: usize, k: usize) -> Result<Vec<Fragment>, String> {
    if k > n || k == 0 { return Err("bad params".into()); }
    let rs = ReedSolomon::new(k, n - k).map_err(|e| format!("{:?}", e))?;
    
    let chunk_size = (data.len() + k - 1) / k;
    let mut shards: Vec<Vec<u8>> = Vec::with_capacity(n);

    // split data into k chunks
    for i in 0..k {
        let start = i * chunk_size;
        let end = std::cmp::min(start + chunk_size, data.len());
        let mut shard = vec![0u8; chunk_size];
        if start < data.len() {
            shard[..(end - start)].copy_from_slice(&data[start..end]);
        }
        shards.push(shard);
    }
    // add parity shards
    for _ in 0..(n - k) { shards.push(vec![0u8; chunk_size]); }

    rs.encode(&mut shards).map_err(|e| format!("{:?}", e))?;
    Ok(shards.into_iter().map(Fragment).collect())
}

// reconstruct original data from fragments
pub fn decode(frags: &[Option<Fragment>], len: usize, n: usize, k: usize) -> Result<Vec<u8>, String> {
    let rs = ReedSolomon::new(k, n - k).map_err(|e| format!("{:?}", e))?;
    let mut shards: Vec<Option<Vec<u8>>> = frags.iter().map(|f| f.as_ref().map(|x| x.0.clone())).collect();
    rs.reconstruct(&mut shards).map_err(|e| format!("{:?}", e))?;

    let mut out = Vec::new();
    for i in 0..k {
        if let Some(s) = &shards[i] { out.extend_from_slice(s); }
    }
    out.truncate(len);
    Ok(out)
}
