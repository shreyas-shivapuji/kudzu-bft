use crate::voting::{BlockProp, FirstVote, FinalVote, NotarVote};
use crate::ReplicaId;
use serde::{Serialize, Deserialize};
use std::net::SocketAddr;
use tokio::net::UdpSocket;
use std::sync::Arc;

const MTU: usize = 65000;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Msg {
    Proposal(BlockProp),
    First(FirstVote),
    Final(FinalVote),
    Notar(NotarVote),
}

pub struct UdpNet {
    socket: Arc<UdpSocket>,
    peers: Vec<SocketAddr>,
    id: ReplicaId,
}

impl UdpNet {
    pub async fn new(port: u16, peers: Vec<SocketAddr>, id: ReplicaId) -> std::io::Result<Self> {
        let addr = format!("0.0.0.0:{}", port);
        let socket = UdpSocket::bind(&addr).await?;
        Ok(Self { socket: Arc::new(socket), peers, id })
    }

    pub fn port(&self) -> u16 {
        self.socket.local_addr().unwrap().port()
    }

    pub async fn send(&self, msg: &Msg, to: ReplicaId) -> std::io::Result<usize> {
        let data = bincode::serialize(msg).unwrap();
        self.socket.send_to(&data, self.peers[to]).await
    }

    pub async fn broadcast(&self, msg: &Msg) -> std::io::Result<()> {
        let data = bincode::serialize(msg).unwrap();
        for (i, peer) in self.peers.iter().enumerate() {
            if i != self.id {
                let _ = self.socket.send_to(&data, peer).await;
            }
        }
        Ok(())
    }

    pub async fn recv(&self) -> std::io::Result<(Msg, SocketAddr)> {
        let mut buf = vec![0u8; MTU];
        let (len, addr) = self.socket.recv_from(&mut buf).await?;
        let msg: Msg = bincode::deserialize(&buf[..len]).map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, e)
        })?;
        Ok((msg, addr))
    }

    pub fn socket(&self) -> Arc<UdpSocket> { self.socket.clone() }
}

pub fn peer_addr(base_port: u16, id: ReplicaId) -> SocketAddr {
    format!("127.0.0.1:{}", base_port + id as u16).parse().unwrap()
}

pub fn peer_addrs(base_port: u16, n: usize) -> Vec<SocketAddr> {
    (0..n).map(|i| peer_addr(base_port, i)).collect()
}
