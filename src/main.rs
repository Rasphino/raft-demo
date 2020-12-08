mod raft_rpc;

use std::sync::{Arc};
use tokio::sync::Mutex;

use crate::raft_rpc::AppendEntriesResponse;
use anyhow::Result;
use async_raft::{raft, AppData, AppDataResponse, Config, RaftError, RaftNetwork};
use async_trait::async_trait;
use memstore::{ClientRequest, ClientResponse, MemStore};
use once_cell::sync::Lazy;
use tonic::{Code, Status};

const PORT_BASE: i32 = 50000;

static RAFT_NODE: Lazy<Mutex<MemRaft>> = Lazy::new(|| {
    println!("Initializing...");
    let node_id = std::env::args().nth(1).expect("Expect node id").parse::<u64>().expect("Invalid node id");
    let group_size = std::env::args().nth(2).expect("Expect group size").parse::<u64>().expect("Invalid group size");
    println!("Node Id: {}, Group Size: {}", node_id, group_size);

    // TODO: initialize grpc server and clients and save them to `network`

    let config = Arc::new(
        Config::build("primary-raft-group".into())
            .validate()
            .expect("Failed to build raft config"),
    );
    let network = Arc::new(Network {});
    let storage = Arc::new(MemStore::new(node_id));
    Mutex::new(MemRaft::new(node_id, config, network, storage))
});

// =======================
// Application Data
// =======================
// `memstore` crate already proved ClientRequest and ClientResponse for us
// TODO: we need to implement our own storage system, and its corresponding application data
// #[derive(Serialize, Deserialize, Debug, Clone)]
// struct ClientRequest {
//     client: String,
//     serial: u64,
//     status: String
// }

// impl AppData for ClientRequest {}

// #[derive(Serialize, Deserialize, Debug, Clone)]
// struct ClientResponse(std::result::Result<Option<String>, ClientError>);

// impl AppDataResponse for ClientResponse {}

// #[derive(Serialize, Deserialize, Debug, Clone)]
// enum ClientError {
//     /// This request has already been applied to the state machine, and the original response
//     /// no longer exists.
//     OldRequestReplayed,
// }

// =======================
// Network
// =======================
struct Network {
    // TODO: save grpc clients and server
    // client:
    // server:
}

#[async_raft::async_trait::async_trait]
impl RaftNetwork<memstore::ClientRequest> for Network {
    // TODO: Implement these boilerplate trait functions
    async fn append_entries(
        &self,
        target: async_raft::NodeId,
        rpc: raft::AppendEntriesRequest<ClientRequest>,
    ) -> Result<raft::AppendEntriesResponse> {
        unimplemented!()
    }

    async fn install_snapshot(
        &self,
        target: async_raft::NodeId,
        rpc: raft::InstallSnapshotRequest,
    ) -> Result<raft::InstallSnapshotResponse> {
        unimplemented!()
    }

    async fn vote(
        &self,
        target: async_raft::NodeId,
        rpc: async_raft::raft::VoteRequest,
    ) -> Result<async_raft::raft::VoteResponse> {
        unimplemented!()
    }
}

#[derive(Debug, Default)]
struct RpcServer {}

#[async_trait]
impl raft_rpc::raft_server::Raft for RpcServer {
    // FIXME: Maybe we write some macros to remove these shitty boilerplate.
    async fn append_entries(
        &self,
        request: tonic::Request<raft_rpc::AppendEntriesRequest>,
    ) -> std::result::Result<tonic::Response<raft_rpc::AppendEntriesResponse>, tonic::Status> {
        let request = request.into_inner();
        let request = raft::AppendEntriesRequest {
            term: (&request).term,
            leader_id: (&request).leader_id,
            prev_log_index: (&request).prev_log_index,
            prev_log_term: (&request).prev_log_term,
            entries: serde_json::from_str(&request.entries).unwrap(),
            leader_commit: (&request).leader_commit,
        };

        let res = {
            let raft = RAFT_NODE.lock().await;
            raft.append_entries(request).await
        };
        match res.map(|res| raft_rpc::AppendEntriesResponse {
                term: res.term,
                success: res.success,
                conflict_opt: serde_json::to_string(&res.conflict_opt).unwrap_or("".to_string()),
            }) {
            Ok(r) => Ok(tonic::Response::new(r)),
            Err(e) => Err(Status::new(Code::Internal, format!("{:?}", e))),
        }
    }

    async fn install_snapshot(
        &self,
        request: tonic::Request<raft_rpc::InstallSnapshotRequest>,
    ) -> std::result::Result<tonic::Response<raft_rpc::InstallSnapshotResponse>, tonic::Status>
    {
        let request = request.into_inner();
        let request = raft::InstallSnapshotRequest {
            term: (&request).term,
            leader_id: (&request).leader_id,
            last_included_index: (&request).last_included_index,
            last_included_term: (&request).last_included_term,
            offset: (&request).offset,
            data: (&request).data.clone(),
            done: (&request).done,
        };
        let res = {
            let raft = RAFT_NODE.lock().await;
            raft.install_snapshot(request).await
        };
        match res.map(|res| raft_rpc::InstallSnapshotResponse { term: res.term }) {
            Ok(r) => Ok(tonic::Response::new(r)),
            Err(e) => Err(Status::new(Code::Internal, format!("{:?}", e))),
        }
    }

    async fn vote(
        &self,
        request: tonic::Request<raft_rpc::VoteRequest>,
    ) -> std::result::Result<tonic::Response<raft_rpc::VoteResponse>, tonic::Status> {
        let request = request.into_inner();
        let request = raft::VoteRequest {
            term: (&request).term,
            candidate_id: (&request).candidate_id,
            last_log_index: (&request).last_log_index,
            last_log_term: (&request).last_log_term
        };
        let res = RAFT_NODE.lock().await.vote(request).await;
        match res.map(|res| raft_rpc::VoteResponse { term: res.term, vote_granted: res.vote_granted })
        {
            Ok(r) => Ok(tonic::Response::new(r)),
            Err(e) => Err(Status::new(Code::Internal, format!("{:?}", e))),
        }
    }
}

type MemRaft = async_raft::Raft<ClientRequest, ClientResponse, Network, memstore::MemStore>;

#[tokio::main]
async fn main() {
    println!("Hello, world!");

    // TODO: What can we do with the raft node? How can we implement our business logic based on the provided raft api?
}
