mod raft_rpc;

use std::sync::Mutex;

use anyhow::Result;
use async_trait::async_trait;
use async_raft::{AppData, AppDataResponse, RaftNetwork, raft};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};

// static RAFT_NODE: Lazy<Mutex<MemRaft>> = Lazy::new(|| {
//     Mutex::new(MemRaft::new(1, config, network, storage))
// });

// =======================
// Application Data
// =======================
#[derive(Serialize, Deserialize, Debug, Clone)]
struct ClientRequest {
    client: String,
    serial: u64,
    status: String
}

impl AppData for ClientRequest {}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct ClientResponse(std::result::Result<Option<String>, ClientError>);

impl AppDataResponse for ClientResponse {}

#[derive(Serialize, Deserialize, Debug, Clone)]
enum ClientError {
    /// This request has already been applied to the state machine, and the original response
    /// no longer exists.
    OldRequestReplayed,
}



// =======================
// Network
// =======================
struct Network {}

#[async_trait]
impl RaftNetwork<ClientRequest> for Network {
    async fn append_entries(&self, target: async_raft::NodeId, rpc: raft::AppendEntriesRequest<ClientRequest>) -> Result<raft::AppendEntriesResponse> {
        unimplemented!()
    }

    async fn install_snapshot(&self, target: async_raft::NodeId, rpc: raft::InstallSnapshotRequest) -> Result<raft::InstallSnapshotResponse> {
        unimplemented!()
    }

    async fn vote(&self, target: async_raft::NodeId, rpc: async_raft::raft::VoteRequest) -> Result<async_raft::raft::VoteResponse> {
        unimplemented!()
    }
}

#[derive(Debug, Default)]
struct RpcServer {
}

#[async_trait]
impl raft_rpc::raft_server::Raft for RpcServer {
    async fn append_entries(
            &self,
            request: tonic::Request<raft_rpc::AppendEntriesRequest>,
        ) -> std::result::Result<tonic::Response<raft_rpc::AppendEntriesResponse>, tonic::Status> {
        let request = raft::AppendEntriesRequest {
            term: request.into_inner().term,
            leader_id: request.into_inner().leader_id,
            prev_log_index: request.into_inner().prev_log_index,
            prev_log_term: request.into_inner().prev_log_term,
            entries: serde_json::from_str(&request.into_inner().entries).unwrap(),
            leader_commit: request.into_inner().leader_commit
        };
        // raft.append_entries(...)
        let response = raft_rpc::AppendEntriesResponse {
            term: (),
            success: (),
            conflict_opt: ()           
        };
        Ok(tonic::Response::new(response))
    }

    async fn install_snapshot(
            &self,
            request: tonic::Request<raft_rpc::InstallSnapshotRequest>,
        ) -> std::result::Result<tonic::Response<raft_rpc::InstallSnapshotResponse>, tonic::Status> {
        todo!()
    }

    async fn vote(
            &self,
            request: tonic::Request<raft_rpc::VoteRequest>,
        ) -> std::result::Result<tonic::Response<raft_rpc::VoteResponse>, tonic::Status> {
        todo!()
    }
}




type MemRaft = async_raft::Raft<ClientRequest, ClientResponse, Network, memstore::MemStore>;


fn main() {
    println!("Hello, world!");
}
