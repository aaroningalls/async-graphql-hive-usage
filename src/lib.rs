use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex, MutexGuard},
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use async_graphql::{
    Response, ServerResult, Value, Variables,
    extensions::{
        Extension, ExtensionContext, ExtensionFactory, NextExecute, NextParseQuery, NextRequest,
        NextResolve, NextSubscribe, ResolveInfo,
    },
    futures_util::stream::BoxStream,
    parser::types::ExecutableDocument,
};
use base64::{Engine, prelude::BASE64_STANDARD};
use serde::Serialize;

use xxhash_rust::xxh3::xxh3_128;

#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Report {
    size: i32,
    map: HashMap<String, OperationMapRecord>,
    operations: Option<Vec<RequestOperation>>,
    subscription_operations: Option<Vec<SubscriptionOperation>>,
}

#[derive(Serialize, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
struct OperationMapRecord {
    operation: String,
    operation_name: Option<String>,
    fields: Vec<String>,
}

#[derive(Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
struct RequestOperation {
    timestamp: u128,
    operation_map_key: String,
    execution: Execution,
    metadata: Option<Metadata>,
    persisted_document_hash: Option<String>,
}

#[derive(Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
struct Execution {
    ok: bool,
    duration: u128,
    errors_total: i32,
}

#[derive(Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
struct SubscriptionOperation {
    timestamp: u128,
    operation_map_key: String,
    metadata: Option<Metadata>,
}

#[derive(Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
struct Metadata {
    client: Option<Client>,
}

#[derive(Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
struct Client {
    name: String,
    version: String,
}

pub trait HiveSender {
    fn send(&self, report: Report);
    fn should_send(&self, report: &Report) -> bool {
        let mut buf = Vec::with_capacity(4096);

        let vec = serde_json::to_writer(&mut buf, report);

        if vec.is_err() {
            return false;
        }

        buf.len() > 1_000_000
    }
}

pub struct HiveUsageClient<S: HiveSender> {
    sender: Arc<S>,
    buffer: Arc<Mutex<HiveUsageBuffer>>,
}

#[derive(Default, Debug)]
struct HiveUsageBuffer {
    map: HashMap<String, OperationMapRecord>,
    operations: Vec<RequestOperation>,
    subscriptions: Vec<SubscriptionOperation>,
}

impl From<MutexGuard<'_, HiveUsageBuffer>> for Report {
    fn from(value: MutexGuard<'_, HiveUsageBuffer>) -> Self {
        Report {
            size: value.map.len() as i32,
            map: value.map.clone(),
            operations: if value.operations.is_empty() {
                None
            } else {
                Some(value.operations.clone())
            },
            subscription_operations: if value.subscriptions.is_empty() {
                None
            } else {
                Some(value.subscriptions.clone())
            },
        }
    }
}

impl<S: HiveSender> HiveUsageClient<S> {
    pub fn new(sender: S) -> Self {
        HiveUsageClient {
            sender: Arc::new(sender),
            buffer: Arc::new(Mutex::new(HiveUsageBuffer::default())),
        }
    }

    pub fn clear(&self) {
        if let Ok(mut buffer) = self.buffer.lock() {
            std::mem::take(&mut *buffer);
        }
    }
}

struct HiveUsageExtension<S: HiveSender> {
    state: Mutex<HiveUsageState>,
    client: Arc<Mutex<HiveUsageBuffer>>,
    sender: Arc<S>,
}

#[derive(Default)]
struct HiveUsageState {
    hash_key: Option<String>,
    operation: Option<String>,
    fields: HashSet<String>,
    subscription: bool,
}

impl<S: HiveSender + Send + Sync + 'static> ExtensionFactory for HiveUsageClient<S> {
    fn create(&self) -> std::sync::Arc<dyn Extension> {
        Arc::new(HiveUsageExtension {
            state: Mutex::new(HiveUsageState::default()),
            client: self.buffer.clone(),
            sender: self.sender.clone(),
        })
    }
}

#[async_graphql::async_trait::async_trait]
impl<S: HiveSender + Send + Sync + 'static> Extension for HiveUsageExtension<S> {
    fn subscribe<'s>(
        &self,
        ctx: &ExtensionContext<'_>,
        stream: BoxStream<'s, Response>,
        next: NextSubscribe<'_>,
    ) -> BoxStream<'s, Response> {
        if let Ok(mut state) = self.state.lock() {
            state.subscription = true
        }

        next.run(ctx, stream)
    }

    async fn parse_query(
        &self,
        ctx: &ExtensionContext<'_>,
        query: &str,
        variables: &Variables,
        next: NextParseQuery<'_>,
    ) -> ServerResult<ExecutableDocument> {
        let result = next.run(ctx, query, variables).await?;

        if let Ok(mut state) = self.state.lock() {
            state.hash_key = Some(BASE64_STANDARD.encode(xxh3_128(query.as_bytes()).to_ne_bytes()));
            state.operation = Some(query.to_string());
        }

        Ok(result)
    }

    async fn execute(
        &self,
        ctx: &ExtensionContext<'_>,
        operation_name: Option<&str>,
        next: NextExecute<'_>,
    ) -> Response {
        let start = Instant::now();

        let response = next.run(ctx, operation_name).await;

        let duration = Instant::now() - start;

        if let (Ok(state), Ok(mut buffer)) = (self.state.lock(), self.client.lock())
            && let (Some(key), Some(operation), false) =
                (&state.hash_key, &state.operation, state.fields.is_empty())
        {
            buffer.map.insert(
                key.clone(),
                OperationMapRecord {
                    operation: operation.clone(),
                    operation_name: operation_name.map(|e| e.to_string()),
                    fields: state.fields.iter().cloned().collect::<Vec<_>>(),
                },
            );

            if state.subscription {
                let request = SubscriptionOperation {
                    timestamp: SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_millis(),
                    operation_map_key: key.clone(),
                    metadata: None,
                };

                buffer.subscriptions.push(request);
            } else {
                let request = RequestOperation {
                    timestamp: SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_millis(),
                    operation_map_key: key.clone(),
                    metadata: None,
                    persisted_document_hash: None,
                    execution: Execution {
                        ok: response.is_ok(),
                        duration: duration.as_nanos(),
                        errors_total: response.errors.len() as i32,
                    },
                };

                buffer.operations.push(request);
            }
        }

        response
    }

    async fn resolve(
        &self,
        ctx: &ExtensionContext<'_>,
        info: ResolveInfo<'_>,
        next: NextResolve<'_>,
    ) -> ServerResult<Option<Value>> {
        if let Ok(mut state) = self.state.lock() {
            state.fields.insert(info.parent_type.to_string());
            state
                .fields
                .insert(format!("{}.{}", info.parent_type, info.name));
        }

        let result = next.run(ctx, info).await?;

        Ok(result)
    }

    async fn request(&self, ctx: &ExtensionContext<'_>, next: NextRequest<'_>) -> Response {
        let response = next.run(ctx).await;

        if let Ok(buffer) = self.client.lock() {
            let report: Report = buffer.into();
            if self.sender.should_send(&report) {
                self.sender.send(report);
            }
        }

        response
    }
}
