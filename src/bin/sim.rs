use clap::Parser;
use std::collections::HashMap;
use std::io::{BufRead, BufReader};
use std::process::{Command, Child, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};
use std::thread;

#[derive(Parser)]
struct Args {
    #[arg(short = 'n', default_value = "7")]
    n: usize,
    #[arg(short = 'f', default_value = "2")]
    f: usize,
    #[arg(short = 'p', default_value = "0")]
    p: usize,
    #[arg(long, default_value = "10000")]
    base_port: u16,
    #[arg(short = 's', default_value = "5")]
    slots: u64,
    #[arg(short = 'c', default_value = "0")]
    crashes: usize,
    #[arg(long)]
    verbose: bool,
}

fn main() {
    let args = Args::parse();
    println!();
    println!("KUDZU SIMULATION");
    println!("n={} f={} p={} slots={} crashes={}", args.n, args.f, args.p, args.slots, args.crashes);
    
    // sanity check
    if args.n < 3 * args.f + 2 * args.p + 1 {
        eprintln!("error: n must be >= 3f + 2p + 1 (got n={}, need {})", 
            args.n, 3 * args.f + 2 * args.p + 1);
        return;
    }
    
    println!("quorum={} fast_quorum={} k={}", 
        args.n - args.f - args.p, args.n - args.p, args.f + args.p + 1);
    println!();
    
    let start = Instant::now();
    let (tx, rx) = mpsc::channel::<(usize, String, bool)>();
    let mut children: Vec<Child> = Vec::new();
    let mut handles = Vec::new();
    
    // spawn all the nodes
    for id in 0..args.n {
        // simulate crashes by not spawning some nodes
        if id >= args.n - args.crashes {
            println!("node {} crashed", id);
            continue;
        }
        
        let mut child = Command::new("cargo")
            .args([
                "run", "--release", "--bin", "node", "-q", "--",
                "-i", &id.to_string(),
                "-n", &args.n.to_string(),
                "-f", &args.f.to_string(),
                "-p", &args.p.to_string(),
                "--base-port", &args.base_port.to_string(),
                "-s", &args.slots.to_string(),
            ])
            .env("RUST_LOG", if args.verbose { "debug" } else { "info" })
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("failed to spawn node");
        
        // read stderr in separate thread
        let stderr = child.stderr.take().unwrap();
        let tx_clone = tx.clone();
        let handle = thread::spawn(move || {
            let reader = BufReader::new(stderr);
            for line in reader.lines().flatten() {
                let important = line.contains("INFO") || line.contains("WARN");
                let _ = tx_clone.send((id, line, important));
            }
        });
        handles.push(handle);
        
        // also read stdout
        let stdout = child.stdout.take().unwrap();
        let tx2 = tx.clone();
        let h2 = thread::spawn(move || {
            let reader = BufReader::new(stdout);
            for line in reader.lines().flatten() {
                let _ = tx2.send((id, line, true));
            }
        });
        handles.push(h2);
        
        children.push(child);
        thread::sleep(Duration::from_millis(100));
    }
    
    drop(tx); // close sender so rx will terminate
    
    let mut results: Vec<Vec<String>> = vec![Vec::new(); args.n];
    let mut finalized: HashMap<usize, Vec<(u64, String)>> = HashMap::new();
    
    // collect all output
    for (id, line, important) in rx {
        // parse finalized blocks from output
        if line.trim().starts_with("slot ") {
            let parts: Vec<&str> = line.trim().split(':').collect();
            if parts.len() == 2 {
                if let Ok(slot) = parts[0].trim().strip_prefix("slot ").unwrap_or("0").parse::<u64>() {
                    finalized.entry(id).or_default().push((slot, parts[1].trim().to_string()));
                }
            }
        }
        
        // save metrics/results for later
        if line.contains("Results") || line.contains("elapsed") || 
           line.contains("msgs:") || line.contains("bytes:") || 
           line.contains("Finalization:") || line.contains("Slot duration:") {
            results[id].push(line.clone());
            continue;
        }
        
        // print logs with colors
        if important || args.verbose {
            let msg = line.find("] ").map(|p| &line[p+2..]).unwrap_or(&line);
            
            if msg.contains("FAST FINALIZED") {
                println!("\x1b[32m{}\x1b[0m", msg);
            } else if msg.contains("SLOW FINALIZED") {
                println!("\x1b[33m{}\x1b[0m", msg);
            } else if msg.contains("PROPOSED") {
                println!("\x1b[36m{}\x1b[0m", msg);
            } else if msg.contains("TIMED OUT") {
                println!("\x1b[31m{}\x1b[0m", msg);
            } else if msg.contains("FIRST-VOTE") {
                println!("\x1b[34m{}\x1b[0m", msg);
            } else if !msg.contains("Finalized") && !msg.trim().starts_with("slot ") {
                println!("{}", msg);
            }
        }
    }
    
    // wait for everything to finish
    for mut child in children {
        let _ = child.wait();
    }
    for handle in handles {
        let _ = handle.join();
    }
    
    let elapsed = start.elapsed();
    
    // === print results ===
    println!();
    println!("--- SIMULATION RESULTS ---");
    for lines in results.iter() {
        if !lines.is_empty() {
            for line in lines { println!("{}", line); }
            println!();
        }
    }
    
    println!("----- FINALIZED BLOCKS -----");
    for id in 0..args.n {
        if id >= args.n - args.crashes {
            println!("node {:>2}: [crashed]", id);
            continue;
        }
        
        if let Some(blocks) = finalized.get(&id) {
            let mut sorted = blocks.clone();
            sorted.sort_by_key(|(s, _)| *s);
            let pairs: Vec<String> = sorted.iter().map(|(s, h)| format!("s{}:{}", s, h)).collect();
            println!("node {:>2}: [{}]", id, pairs.join(", "));
        } else {
            println!("node {:>2}: []", id);
        }
    }
    
    // check for consensus violations
    println!();
    let mut slot_hash: HashMap<u64, String> = HashMap::new();
    let mut conflict = false;
    let mut max_fin = 0;
    
    for id in 0..args.n - args.crashes {
        if let Some(blocks) = finalized.get(&id) {
            max_fin = max_fin.max(blocks.len());
            for (slot, hash) in blocks {
                if let Some(existing) = slot_hash.get(slot) {
                    if existing != hash {
                        println!("\x1b[31mconflict at slot {}: {} vs {}\x1b[0m", slot, hash, existing);
                        conflict = true;
                    }
                } else {
                    slot_hash.insert(*slot, hash.clone());
                }
            }
        }
    }
    
    // figure out which nodes are lagging
    let lagging: Vec<usize> = (0..args.n - args.crashes)
        .filter(|id| finalized.get(id).map(|b| b.len()).unwrap_or(0) < max_fin)
        .collect();
    
    if conflict {
        println!("\x1b[31mSafety Violation!\x1b[0m");
    } else if !lagging.is_empty() {
        println!("\x1b[33mConsensus Ok, {} lagging: {:?}\x1b[0m", lagging.len(), lagging);
    } else if max_fin > 0 {
        println!("\x1b[32mConsensus Achieved, {} blocks\x1b[0m", max_fin);
    } else {
        println!("\x1b[31mNo Blocks Finalized\x1b[0m");
    }
    
    println!("time: {:.2}s", elapsed.as_secs_f64());
}
