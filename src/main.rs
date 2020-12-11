mod raft_rpc;

use std::sync::{Arc};
use raft_rpc::raft_server::RaftServer;
use tokio::sync::Mutex;

use tonic::{transport::Server};
use anyhow::{Result, Context};
use async_raft::{raft, AppData, AppDataResponse, Config, RaftError, RaftNetwork};
use async_trait::async_trait;
use memstore::{ClientRequest, ClientResponse, MemStore};
use once_cell::sync::{Lazy, OnceCell};
use tonic::{Code, Status};
use tonic::codegen::Future;
use crate::raft_rpc::raft_client::RaftClient;
use tonic::transport::Channel;
use crate::raft_rpc::{AppendEntriesRequest, InstallSnapshotRequest, VoteRequest};
use async_raft::raft::{AppendEntriesResponse, InstallSnapshotResponse, VoteResponse, ClientWriteRequest};
use std::cell::RefCell;
use std::ops::Deref;
use slog::{error, info, o, Drain, Logger};
use std::thread;
use std::thread::sleep;
use tokio::time::Duration;
use std::collections::HashSet;

const PORT_BASE: u64 = 50000;

static RAFT_NODE: OnceCell<Mutex<MemRaft>> = OnceCell::new();

static LOGGER: Lazy<Logger> = Lazy::new(|| {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    slog::Logger::root(drain, o!())
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
//
// impl AppData for ClientRequest {}
//
// #[derive(Serialize, Deserialize, Debug, Clone)]
// struct ClientResponse(std::result::Result<Option<String>, ClientError>);
//
// impl AppDataResponse for ClientResponse {}
//
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
    clients: Mutex<Vec<RaftClient<Channel>>>
}

#[async_raft::async_trait::async_trait]
impl RaftNetwork<memstore::ClientRequest> for Network {
    async fn append_entries(
        &self,
        target: async_raft::NodeId,
        rpc: raft::AppendEntriesRequest<ClientRequest>,
    ) -> Result<raft::AppendEntriesResponse> {
        info!(LOGGER, "CLIENT send append_entries request to {}: {:?}", target, rpc);
        let request = tonic::Request::new(AppendEntriesRequest {
            term: rpc.term,
            leader_id: rpc.leader_id,
            prev_log_index: rpc.prev_log_index,
            prev_log_term: rpc.prev_log_term,
            entries: serde_json::to_string(&rpc.entries).unwrap_or("".to_string()),
            leader_commit: rpc.leader_commit
        });

        let response = {
            let mut lock = self.clients.lock().await;
            let client = (*lock).get_mut(target as usize).unwrap();

            client.append_entries(request).await
        };

        info!(LOGGER, "CLIENT received append_entries response from {}: {:?}", target, response);

        match response {
            Ok(response) => {
                let response = response.into_inner();
                Ok(AppendEntriesResponse {
                    term: (&response).term,
                    success: (&response).success,
                    conflict_opt: serde_json::from_str(&(&response).conflict_opt).ok()
                })
            },
            Err(e) => {
                Err(anyhow::Error::new(e))
            }
        }
    }

    async fn install_snapshot(
        &self,
        target: async_raft::NodeId,
        rpc: raft::InstallSnapshotRequest,
    ) -> Result<raft::InstallSnapshotResponse> {
        let request = tonic::Request::new(InstallSnapshotRequest {
            term: rpc.term,
            leader_id: rpc.leader_id,
            last_included_index: rpc.last_included_index,
            last_included_term: rpc.last_included_term,
            offset: rpc.offset,
            data: rpc.data,
            done: rpc.done
        });

        let response = {
            let mut lock = self.clients.lock().await;
            let client = (*lock).get_mut(target as usize).unwrap();

            client.install_snapshot(request).await
        };

        match response {
            Ok(response) => {
                let response = response.into_inner();
                Ok(InstallSnapshotResponse {
                    term: (&response).term,
                })
            },
            Err(e) => {
                Err(anyhow::Error::new(e))
            }
        }
    }

    async fn vote(
        &self,
        target: async_raft::NodeId,
        rpc: async_raft::raft::VoteRequest,
    ) -> Result<async_raft::raft::VoteResponse> {
        info!(LOGGER, "CLIENT send vote request to {}: {:?}", target, rpc);
        let request = tonic::Request::new(VoteRequest {
            term: rpc.term,
            candidate_id: rpc.candidate_id,
            last_log_index: rpc.last_log_index,
            last_log_term: rpc.last_log_term
        });

        let response = {
            let mut lock = self.clients.lock().await;
            let client = (*lock).get_mut(target as usize)
                .with_context(|| format!("Target: {}", target)).unwrap();

            client.vote(request).await
        };

        info!(LOGGER, "CLIENT received vote response from {}: {:?}", target, response);

        match response {
            Ok(response) => {
                let response = response.into_inner();
                Ok(VoteResponse {
                    term: (&response).term,
                    vote_granted: (&response).vote_granted
                })
            },
            Err(e) => {
                Err(anyhow::Error::new(e))
            }
        }
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
            let raft = RAFT_NODE.get().unwrap().lock().await;
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
            let raft = RAFT_NODE.get().unwrap().lock().await;
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
        info!(LOGGER, "Send vote request: {:?}", request);
        let request = request.into_inner();
        let request = raft::VoteRequest {
            term: (&request).term,
            candidate_id: (&request).candidate_id,
            last_log_index: (&request).last_log_index,
            last_log_term: (&request).last_log_term
        };
        let res = RAFT_NODE.get().unwrap().lock().await.vote(request).await;
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
    println!("Initializing...");
    let node_id = std::env::args().nth(1).expect("Expect node id").parse::<u64>().expect("Invalid node id");
    let group_size = std::env::args().nth(2).expect("Expect group size").parse::<u64>().expect("Invalid group size");
    println!("Node Id: {}, Group Size: {}", node_id, group_size);

    // start grpc server
    let addr = format!("127.0.0.1:{}", PORT_BASE + node_id).parse().unwrap();
    let rpc_server = RpcServer::default();
    let rpc_server_thread =  thread::spawn(move || {
        let mut runtime = tokio::runtime::Runtime::new().expect("Unable to create the runtime");
        info!(LOGGER, "Start grpc server!");
        runtime.block_on(async {
            Server::builder()
                .add_service(RaftServer::new(rpc_server))
                .serve(addr).await.unwrap();
        });
    });

    let mut clients = vec![];
    for i in 0..group_size {
        loop {
            match RaftClient::connect(format!("http://127.0.0.1:{}", PORT_BASE + i)).await {
                Ok(c) => {
                    info!(LOGGER, "Connect to server {} succeed", i);
                    clients.push(c);
                    break
                },
                Err(_) => {
                    info!(LOGGER, "Connect to server {} failed, wait and retry", i);
                    sleep(Duration::from_secs(1));
                    continue
                }
            }
        }
    }

    let config = Arc::new(
        Config::build("primary-raft-group".into())
            .validate()
            .expect("Failed to build raft config"),
    );
    let network = Arc::new(Network { clients: Mutex::new(clients) });
    let storage = Arc::new(MemStore::new(node_id));
    let raft = Mutex::new(MemRaft::new(node_id, config, network, storage));
    RAFT_NODE.set(raft);

    let mut members = HashSet::new();
    for id in 0..group_size {
        members.insert(id);
    }

    // sleep(Duration::from_secs(2));

    RAFT_NODE.get().unwrap().lock().await.initialize(members).await.unwrap();
    // TODO: What can we do with the raft node? How can we implement our business logic based on the provided raft api?
    // if node_id == 0 {
    //     info!(LOGGER, "Send request");
    //     let resp =  RAFT_NODE.get().unwrap().lock().await.client_write(ClientWriteRequest::new(ClientRequest {
    //         client: "client1".to_string(),
    //         serial: 0,
    //         status: "hello".to_string()
    //     })).await.unwrap();
    //
    //     info!(LOGGER, "{:?}", resp);
    // }

    rpc_server_thread.join();

}
