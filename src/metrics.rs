use std::time::{Duration, Instant};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

#[derive(Default)]
pub struct Metrics {
    pub bytes_sent: AtomicU64,
    pub bytes_recv: AtomicU64,
    pub msgs_sent: AtomicUsize,
    pub msgs_recv: AtomicUsize,
    pub slots_finalized: AtomicUsize,
    pub fast_finals: AtomicUsize,
    pub slow_finals: AtomicUsize,
}

impl Metrics {
    pub fn new() -> Self { Self::default() }

    pub fn add_sent(&self, bytes: u64) {
        self.bytes_sent.fetch_add(bytes, Ordering::Relaxed);
        self.msgs_sent.fetch_add(1, Ordering::Relaxed);
    }

    pub fn add_recv(&self, bytes: u64) {
        self.bytes_recv.fetch_add(bytes, Ordering::Relaxed);
        self.msgs_recv.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_final(&self, fast: bool) {
        self.slots_finalized.fetch_add(1, Ordering::Relaxed);
        if fast {
            self.fast_finals.fetch_add(1, Ordering::Relaxed);
        } else {
            self.slow_finals.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn print(&self, elapsed: Duration, id: usize) {
        let sent = self.bytes_sent.load(Ordering::Relaxed);
        let recv = self.bytes_recv.load(Ordering::Relaxed);
        let ms = self.msgs_sent.load(Ordering::Relaxed);
        let mr = self.msgs_recv.load(Ordering::Relaxed);
        let sf = self.slots_finalized.load(Ordering::Relaxed);
        let ff = self.fast_finals.load(Ordering::Relaxed);
        let slow = self.slow_finals.load(Ordering::Relaxed);

        println!("[Node {}] elapsed={:.2}s slots={} fast={} slow={}", 
            id, elapsed.as_secs_f64(), sf, ff, slow);
        println!("         msgs: sent={} recv={}", ms, mr);
        println!("         bytes: sent={} recv={} ({:.2} KB/s)", 
            sent, recv, (sent + recv) as f64 / elapsed.as_secs_f64() / 1024.0);
    }
}

pub struct SlotTimer {
    start: Instant,
    pub fin_latencies: Vec<Duration>,  // time to finalize each slot
    pub slot_durations: Vec<Duration>, // total time in each slot
}

impl SlotTimer {
    pub fn new() -> Self {
        Self { 
            start: Instant::now(), 
            fin_latencies: Vec::new(),
            slot_durations: Vec::new(),
        }
    }

    pub fn reset(&mut self) {
        self.start = Instant::now();
    }

    pub fn record_finalization(&mut self) {
        self.fin_latencies.push(self.start.elapsed());
    }

    pub fn record_slot_exit(&mut self) {
        self.slot_durations.push(self.start.elapsed());
    }

    pub fn avg_fin_latency_ms(&self) -> f64 {
        avg_ms(&self.fin_latencies)
    }

    pub fn avg_slot_duration_ms(&self) -> f64 {
        avg_ms(&self.slot_durations)
    }
}

fn avg_ms(durations: &[Duration]) -> f64 {
    if durations.is_empty() { return 0.0; }
    let sum: Duration = durations.iter().sum();
    sum.as_secs_f64() * 1000.0 / durations.len() as f64
}

impl Default for SlotTimer {
    fn default() -> Self { Self::new() }
}
