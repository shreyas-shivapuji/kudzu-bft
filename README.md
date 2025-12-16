# Kudzu: Simple and High-Throughput BFT Protocol

## Build

```bash
cargo build --release
```

## Usage

### Run Simulation

The simulator spawns multiple nodes locally and runs the consensus protocol:

```bash
cargo run --release --bin sim -- [flags]
```

**Options:**

| Flag | Default | Desc |
|------|---------|-------------|
| `-n` | 7 | Total number of nodes |
| `-f` | 2 | Byzantine fault tolerance |
| `-p` | 0 | Partition tolerance |
| `-s` | 5 | Number of slots to run |
| `-c` | 0 | Number of crashed nodes |
| `--base-port` | 10000 | Starting port number |

**Example:**

```bash
# Run 7 nodes for 10 slots
cargo run --release --bin sim -- -n 7 -f 2 -s 10

# Simulate with 1 crashed node
cargo run --release --bin sim -- -n 7 -f 2 -s 5 -c 1
```

### Run Single Node

For manual testing or distributed deployment:

```bash
cargo run --release --bin node -- [flags]
```

**Options:**

| Flag | Default | Desc |
|------|---------|-------------|
| `-i` | required | Node ID (0 to n-1) |
| `-n` | 7 | Total number of nodes |
| `-f` | 2 | Byzantine fault tolerance |
| `-p` | 0 | Partition tolerance |
| `-s` | 5 | Number of slots |
| `--base-port` | 10000 | Starting port number |

**Example:**

```bash
# Start node 0 in a 7-node network
cargo run --release --bin node -- -i 0 -n 7 -f 2 -s 10
```

## Protocol Parameters

The protocol requires `n ≥ 3f + 2p + 1` where:
- `n` = total nodes
- `f` = max Byzantine faults
- `p` = max partitioned nodes

**Quorums:**
- Standard quorum: `n - f - p`
- Fast quorum: `n - p`
- Reconstruction threshold: `f + p + 1`

