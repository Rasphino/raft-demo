#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AppendEntriesRequest {
    #[prost(uint64, tag = "1")]
    pub term: u64,
    #[prost(uint64, tag = "2")]
    pub leader_id: u64,
    #[prost(uint64, tag = "3")]
    pub prev_log_index: u64,
    #[prost(uint64, tag = "4")]
    pub prev_log_term: u64,
    /// serialize
    #[prost(string, tag = "6")]
    pub entries: std::string::String,
    #[prost(uint64, tag = "5")]
    pub leader_commit: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AppendEntriesResponse {
    #[prost(uint64, tag = "1")]
    pub term: u64,
    #[prost(bool, tag = "2")]
    pub success: bool,
    /// optional & serialize
    #[prost(string, tag = "3")]
    pub conflict_opt: std::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct InstallSnapshotRequest {
    #[prost(uint64, tag = "1")]
    pub term: u64,
    #[prost(uint64, tag = "2")]
    pub leader_id: u64,
    #[prost(uint64, tag = "3")]
    pub last_included_index: u64,
    #[prost(uint64, tag = "4")]
    pub last_included_term: u64,
    #[prost(uint64, tag = "5")]
    pub offset: u64,
    #[prost(bytes, tag = "7")]
    pub data: std::vec::Vec<u8>,
    #[prost(bool, tag = "6")]
    pub done: bool,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct InstallSnapshotResponse {
    #[prost(uint64, tag = "1")]
    pub term: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct VoteRequest {
    #[prost(uint64, tag = "1")]
    pub term: u64,
    #[prost(uint64, tag = "2")]
    pub candidate_id: u64,
    #[prost(uint64, tag = "3")]
    pub last_log_index: u64,
    #[prost(uint64, tag = "4")]
    pub last_log_term: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct VoteResponse {
    #[prost(uint64, tag = "1")]
    pub term: u64,
    #[prost(bool, tag = "2")]
    pub vote_granted: bool,
}
#[doc = r" Generated client implementations."]
pub mod raft_client {
    #![allow(unused_variables, dead_code, missing_docs)]
    use tonic::codegen::*;
    pub struct RaftClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl RaftClient<tonic::transport::Channel> {
        #[doc = r" Attempt to create a new client by connecting to a given endpoint."]
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: std::convert::TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> RaftClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::ResponseBody: Body + HttpBody + Send + 'static,
        T::Error: Into<StdError>,
        <T::ResponseBody as HttpBody>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_interceptor(inner: T, interceptor: impl Into<tonic::Interceptor>) -> Self {
            let inner = tonic::client::Grpc::with_interceptor(inner, interceptor);
            Self { inner }
        }
        pub async fn append_entries(
            &mut self,
            request: impl tonic::IntoRequest<super::AppendEntriesRequest>,
        ) -> Result<tonic::Response<super::AppendEntriesResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/raft.Raft/AppendEntries");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn install_snapshot(
            &mut self,
            request: impl tonic::IntoRequest<super::InstallSnapshotRequest>,
        ) -> Result<tonic::Response<super::InstallSnapshotResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/raft.Raft/InstallSnapshot");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn vote(
            &mut self,
            request: impl tonic::IntoRequest<super::VoteRequest>,
        ) -> Result<tonic::Response<super::VoteResponse>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/raft.Raft/Vote");
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
    impl<T: Clone> Clone for RaftClient<T> {
        fn clone(&self) -> Self {
            Self {
                inner: self.inner.clone(),
            }
        }
    }
    impl<T> std::fmt::Debug for RaftClient<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "RaftClient {{ ... }}")
        }
    }
}
#[doc = r" Generated server implementations."]
pub mod raft_server {
    #![allow(unused_variables, dead_code, missing_docs)]
    use tonic::codegen::*;
    #[doc = "Generated trait containing gRPC methods that should be implemented for use with RaftServer."]
    #[async_trait]
    pub trait Raft: Send + Sync + 'static {
        async fn append_entries(
            &self,
            request: tonic::Request<super::AppendEntriesRequest>,
        ) -> Result<tonic::Response<super::AppendEntriesResponse>, tonic::Status>;
        async fn install_snapshot(
            &self,
            request: tonic::Request<super::InstallSnapshotRequest>,
        ) -> Result<tonic::Response<super::InstallSnapshotResponse>, tonic::Status>;
        async fn vote(
            &self,
            request: tonic::Request<super::VoteRequest>,
        ) -> Result<tonic::Response<super::VoteResponse>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct RaftServer<T: Raft> {
        inner: _Inner<T>,
    }
    struct _Inner<T>(Arc<T>, Option<tonic::Interceptor>);
    impl<T: Raft> RaftServer<T> {
        pub fn new(inner: T) -> Self {
            let inner = Arc::new(inner);
            let inner = _Inner(inner, None);
            Self { inner }
        }
        pub fn with_interceptor(inner: T, interceptor: impl Into<tonic::Interceptor>) -> Self {
            let inner = Arc::new(inner);
            let inner = _Inner(inner, Some(interceptor.into()));
            Self { inner }
        }
    }
    impl<T, B> Service<http::Request<B>> for RaftServer<T>
    where
        T: Raft,
        B: HttpBody + Send + Sync + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = Never;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/raft.Raft/AppendEntries" => {
                    #[allow(non_camel_case_types)]
                    struct AppendEntriesSvc<T: Raft>(pub Arc<T>);
                    impl<T: Raft> tonic::server::UnaryService<super::AppendEntriesRequest> for AppendEntriesSvc<T> {
                        type Response = super::AppendEntriesResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::AppendEntriesRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).append_entries(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = AppendEntriesSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/raft.Raft/InstallSnapshot" => {
                    #[allow(non_camel_case_types)]
                    struct InstallSnapshotSvc<T: Raft>(pub Arc<T>);
                    impl<T: Raft> tonic::server::UnaryService<super::InstallSnapshotRequest> for InstallSnapshotSvc<T> {
                        type Response = super::InstallSnapshotResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::InstallSnapshotRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).install_snapshot(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = InstallSnapshotSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/raft.Raft/Vote" => {
                    #[allow(non_camel_case_types)]
                    struct VoteSvc<T: Raft>(pub Arc<T>);
                    impl<T: Raft> tonic::server::UnaryService<super::VoteRequest> for VoteSvc<T> {
                        type Response = super::VoteResponse;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::VoteRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).vote(request).await };
                            Box::pin(fut)
                        }
                    }
                    let inner = self.inner.clone();
                    let fut = async move {
                        let interceptor = inner.1.clone();
                        let inner = inner.0;
                        let method = VoteSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = if let Some(interceptor) = interceptor {
                            tonic::server::Grpc::with_interceptor(codec, interceptor)
                        } else {
                            tonic::server::Grpc::new(codec)
                        };
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => Box::pin(async move {
                    Ok(http::Response::builder()
                        .status(200)
                        .header("grpc-status", "12")
                        .body(tonic::body::BoxBody::empty())
                        .unwrap())
                }),
            }
        }
    }
    impl<T: Raft> Clone for RaftServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self { inner }
        }
    }
    impl<T: Raft> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone(), self.1.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: Raft> tonic::transport::NamedService for RaftServer<T> {
        const NAME: &'static str = "raft.Raft";
    }
}
