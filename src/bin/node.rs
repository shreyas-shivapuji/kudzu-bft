use clap::Parser;
use kudzu_bft::net::{Msg, UdpNet, peer_addrs};
use kudzu_bft::protocol::{Action, Replica, DELTA_TIMEOUT};
use kudzu_bft::voting::FirstVote;
use kudzu_bft::metrics::{Metrics, SlotTimer};
use kudzu_bft::types::leader_for_slot;
use kudzu_bft::Slot;
use log::{debug, info, warn};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::timeout;

#[derive(Parser)]
struct Args {
    #[arg(short, long)]
    id: usize,
    #[arg(short, long, default_value = "7")]
    n: usize,
    #[arg(short, long, default_value = "2")]
    f: usize,
    #[arg(short, long, default_value = "0")]
    p: usize,
    #[arg(long, default_value = "10000")]
    base_port: u16,
    #[arg(short, long, default_value = "5")]
    slots: u64,
}

fn short_hash(h: &kudzu_net::block::BlockHash) -> String {
    hex::encode(&h.0[..4])
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format_timestamp_millis()
        .init();
    
    let args = Args::parse();
    let peers = peer_addrs(args.base_port, args.n);
    let port = args.base_port + args.id as u16;
    
    info!("node {} | starting on port {}", args.id, port);
    
    let net = UdpNet::new(port, peers, args.id).await?;
    let mut replica = Replica::new(args.id, args.n, args.f, args.p);
    let metrics = Arc::new(Metrics::new());
    let mut timer = SlotTimer::new();
    let start = Instant::now();
    let mut finalized: Vec<(u64, String)> = Vec::new();
    
    // wait for other nodes to come up
    tokio::time::sleep(Duration::from_millis(1000)).await;
    
    for slot_num in 1..=args.slots {
        let slot = Slot::new(slot_num);
        let leader = leader_for_slot(slot, args.n);
        timer.reset();
        
        info!("node {} Slot {} (leader={})", args.id, slot_num, leader);
        
        // if we're the leader, propose a block
        if args.id == leader {
            let payload = format!("block_{}_{}", slot_num, args.id);
            match replica.propose(payload.as_bytes()) {
                Ok(acts) => {
                    let hash = replica.state.blocks.values().next()
                        .map(|b| short_hash(&b.hash())).unwrap_or_default();
                    info!("node {} | PROPOSED {} in Slot {}", args.id, hash, slot_num);
                    
                    for act in acts {
                        match act {
                            Action::Send(to, prop) => {
                                let msg = Msg::Proposal(prop);
                                if let Ok(bytes) = net.send(&msg, to).await {
                                    metrics.add_sent(bytes as u64);
                                }
                            }
                            Action::BcastFirst(v) => {
                                let h = short_hash(&v.block_hash());
                                info!("node {} | voted FIRST-VOTE for block {} in Slot {}", args.id, h, slot_num);
                                let _ = net.broadcast(&Msg::First(v)).await;
                                metrics.add_sent(1);
                            }
                            _ => {}
                        }
                    }
                }
                Err(e) => warn!("node {} | propose failed: {}", args.id, e),
            }
        }
        
        // fast path - try to finalize quickly
        let fast_deadline = Instant::now() + Duration::from_millis(100);
        while Instant::now() < fast_deadline && !replica.state.done {
            let t = fast_deadline.saturating_duration_since(Instant::now());
            if let Ok(Ok((msg, _))) = timeout(t, net.recv()).await {
                metrics.add_recv(1);
                for act in handle_msg(&mut replica, msg, args.id, slot_num) {
                    do_action(&net, &metrics, act, args.id, slot_num).await;
                }
                // check if we got enough votes for fast finalization
                if !replica.state.done {
                    if let Some(h) = replica.state.first_votes.many_votes(replica.fast_quorum()).first() {
                        let cnt = replica.state.first_votes.count(h);
                        let hash = short_hash(h);
                        info!("node {} | FAST FINALIZED block {} in Slot {} ({}/{} votes)", 
                            args.id, hash, slot_num, cnt, args.n);
                        let chain = replica.finalize_with_predecessors(h);
                        for b in chain {
                            finalized.push((b.slot.0, short_hash(&b.hash())));
                        }
                        metrics.record_final(true);
                        timer.record_finalization();
                        replica.state.done = true;
                    }
                }
            }
        }
        
        // maybe can still get slow path finalization
        if !replica.state.done {
            if let Some(h) = replica.state.first_votes.many_votes(replica.quorum()).first() {
                let cnt = replica.state.first_votes.count(h);
                let hash = short_hash(h);
                info!("node {} | SLOW FINALIZED block {} in Slot {} ({}/{} votes)", 
                    args.id, hash, slot_num, cnt, args.n);
                let chain = replica.finalize_with_predecessors(h);
                for b in chain {
                    finalized.push((b.slot.0, short_hash(&b.hash())));
                }
                metrics.record_final(false);
                timer.record_finalization();
                replica.state.done = true;
            }
        }
        
        // main loop - keep trying until timeout
        let deadline = Instant::now() + Duration::from_millis(DELTA_TIMEOUT + 400);
        while Instant::now() < deadline && !replica.state.done {
            let t = deadline.saturating_duration_since(Instant::now());
            match timeout(t, net.recv()).await {
                Ok(Ok((msg, _))) => {
                    metrics.add_recv(1);
                    for act in handle_msg(&mut replica, msg, args.id, slot_num) {
                        do_action(&net, &metrics, act, args.id, slot_num).await;
                    }
                    if !replica.state.done {
                        // try fast path first
                        if let Some(h) = replica.state.first_votes.many_votes(replica.fast_quorum()).first() {
                            let cnt = replica.state.first_votes.count(h);
                            let hash = short_hash(h);
                            info!("node {} | FAST FINALIZED block {} in Slot {} ({}/{} votes)", 
                                args.id, hash, slot_num, cnt, args.n);
                            let chain = replica.finalize_with_predecessors(h);
                            for b in chain {
                                finalized.push((b.slot.0, short_hash(&b.hash())));
                            }
                            metrics.record_final(true);
                            timer.record_finalization();
                            replica.state.done = true;
                        } else if let Some(h) = replica.state.first_votes.many_votes(replica.quorum()).first() {
                            // fall back to slow path
                            let cnt = replica.state.first_votes.count(h);
                            let hash = short_hash(h);
                            info!("node {} | SLOW FINALIZED block {} in Slot {} ({}/{} votes)", 
                                args.id, hash, slot_num, cnt, args.n);
                            let chain = replica.finalize_with_predecessors(h);
                            for b in chain {
                                finalized.push((b.slot.0, short_hash(&b.hash())));
                            }
                            metrics.record_final(false);
                            timer.record_finalization();
                            replica.state.done = true;
                        }
                    }
                }
                Ok(Err(e)) => warn!("recv error: {}", e),
                Err(_) => {
                    // timed out waiting for msg, run tick
                    for act in replica.tick(start.elapsed().as_millis() as u64) {
                        do_action(&net, &metrics, act, args.id, slot_num).await;
                    }
                }
            }
        }
        
        if !replica.state.done {
            warn!("node {} | TIMED OUT in Slot {}", args.id, slot_num);
        }
        
        timer.record_slot_exit();
        replica.advance_slot(start.elapsed().as_millis() as u64);
    }
    
    // print results
    let elapsed = start.elapsed();
    println!("\n=== Node {} Results ===", args.id);
    metrics.print(elapsed, args.id);
    println!("Finalization: avg={:.2}ms", timer.avg_fin_latency_ms());
    println!("Slot duration: avg={:.2}ms", timer.avg_slot_duration_ms());
    
    finalized.sort_by_key(|(s, _)| *s);
    finalized.dedup();
    println!("Finalized blocks:");
    for (slot, hash) in &finalized {
        if *slot > 0 {
            println!("  slot {}: {}", slot, hash);
        }
    }
    
    Ok(())
}

fn handle_msg(replica: &mut Replica, msg: Msg, id: usize, _slot: u64) -> Vec<Action> {
    match msg {
        Msg::Proposal(p) => {
            let from = leader_for_slot(p.block.slot, replica.n);
            debug!("node {} | received proposal from leader {}", id, from);
            replica.on_proposal(p, from)
        }
        Msg::First(v) => {
            debug!("node {} | received FIRST-VOTE from node {}", id, v.from());
            replica.on_first_vote(&v, v.from())
        }
        Msg::Final(v) => {
            debug!("node {} | received FINAL-VOTE from node {}", id, v.from);
            replica.on_final_vote(&v);
            Vec::new()
        }
        Msg::Notar(v) => {
            let from = v.from;
            debug!("node {} | received NOTAR-VOTE from node {}", id, from);
            replica.on_first_vote(&FirstVote::new(v), from)
        }
    }
}

async fn do_action(net: &UdpNet, metrics: &Metrics, act: Action, id: usize, slot: u64) {
    match act {
        Action::Send(to, prop) => {
            if let Ok(bytes) = net.send(&Msg::Proposal(prop), to).await {
                metrics.add_sent(bytes as u64);
            }
        }
        Action::BcastFirst(v) => {
            let h = short_hash(&v.block_hash());
            info!("node {} | voted FIRST-VOTE for block {} in Slot {}", id, h, slot);
            let _ = net.broadcast(&Msg::First(v)).await;
            metrics.add_sent(1);
        }
        Action::BcastFinal(v) => {
            let h = short_hash(&v.block_hash);
            debug!("node {} | voted FINAL-VOTE for block {} in Slot {}", id, h, slot);
            let _ = net.broadcast(&Msg::Final(v)).await;
            metrics.add_sent(1);
        }
        Action::BcastNotar(v) => {
            let h = short_hash(&v.block_hash());
            debug!("node {} | voted NOTAR-VOTE for block {} in Slot {}", id, h, slot);
            let _ = net.broadcast(&Msg::Notar(v)).await;
            metrics.add_sent(1);
        }
        Action::Done(h) => {
            debug!("node {} | block {} done in Slot {}", id, short_hash(&h), slot);
        }
    }
}
