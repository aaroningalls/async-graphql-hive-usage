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
use futures::{StreamExt, channel::mpsc};
use serde::Serialize;
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use uuid::Uuid;
use xxhash_rust::xxh3::xxh3_128;

pub mod clients;

#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
/// The serializable report that Hive expects. This should be sent as JSON to your
/// hive endpoint. See https://the-guild.dev/graphql/hive/docs/api-reference/usage-reports
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
pub struct Metadata {
    pub client: Option<Client>,
}

#[derive(Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Client {
    pub name: String,
    pub version: String,
}

/// Struct containing necessary content for the HTTP request
pub struct HiveHTTPRequest {
    /// URL for hive usage (`https://app.graphql-hive.com/usage/<TARGET_ID>`)
    pub url: String,
    /// Authorixation, API version, and request_id headers
    pub headers: Vec<(&'static str, String)>,
    /// Usage report serialized as JSON
    pub body: Vec<u8>,
}

/// Used to send the usage report to hive. It is required to implement `send`, which will perform
/// the actual sending of the data. Optionally implement `should_send` to determine when the request should be sent.
/// Hive encourages batching the requests to limit network operations
pub trait HiveSender {
    /// Send the report
    fn send(&self, report: HiveHTTPRequest) -> impl Future;

    /// A function called every operation to determine if `send` should be called. Default to checking the size
    /// of the JSON payload and sending if size >= 1MB
    fn should_send(&self, report: &Report) -> bool {
        let mut buf = Vec::with_capacity(4096);

        let vec = serde_json::to_writer(&mut buf, report);

        if vec.is_err() {
            return false;
        }

        buf.len() > 1_000_000
    }

    /// Determine how to calculate the request ID. Common use case
    /// is to use the ID elsewhere, the extension simply sends it onwards
    fn request_id(&self) -> Uuid {
        Uuid::new_v4()
    }
}

type MetadataFn = Arc<dyn Fn(&ExtensionContext<'_>) -> Metadata + Send + Sync>;

/// The client is passed to your async-graphql schema builder as an extension.
/// This struct stores the buffered requests until they are sent and cleared
pub struct HiveUsageClient {
    buffer: Arc<Mutex<HiveUsageBuffer>>,
    channel: mpsc::Sender<Report>,
    unstable_subscriptions: bool,
    metadata: Option<MetadataFn>,
}

#[derive(Default, Debug)]
struct HiveUsageBuffer {
    map: HashMap<String, OperationMapRecord>,
    operations: Vec<RequestOperation>,
    subscriptions: Vec<SubscriptionOperation>,
}

impl From<HiveUsageBuffer> for Report {
    fn from(value: HiveUsageBuffer) -> Self {
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

impl HiveUsageBuffer {
    pub fn clear(&mut self) -> HiveUsageBuffer {
        std::mem::take(&mut *self)
    }
}

impl HiveUsageClient {
    pub fn new(sender: impl HiveSender, target_id: Uuid, token: String) -> (Self, impl Future) {
        let (send, mut recv) = mpsc::channel::<Report>(3);

        let task = async move {
            while let Some(report) = recv.next().await {
                if sender.should_send(&report) {
                    let body = match serde_json::to_vec(&report) {
                        Ok(b) => b,
                        Err(_) => continue,
                    };

                    let id = sender.request_id().to_string();

                    #[cfg(feature = "tracing")]
                    {
                        tracing::info!(
                            "Hive Usage ({id}): sending {} operations",
                            report.operations.map(|e| e.len()).unwrap_or(0)
                        )
                    }

                    let req = HiveHTTPRequest {
                        url: format!("https://app.graphql-hive.com/usage/{target_id}"),
                        headers: vec![
                            ("Authorization", format!("Bearer {token}")),
                            ("X-Usage-API-Version", "2".into()),
                            ("X-Request-Id", id),
                            ("Content-Type", "application/json".into()),
                        ],
                        body,
                    };

                    sender.send(req).await;
                }
            }
        };

        (
            HiveUsageClient {
                channel: send,
                buffer: Arc::new(Mutex::new(HiveUsageBuffer::default())),
                unstable_subscriptions: false,
                metadata: None,
            },
            task,
        )
    }

    /// Because of a quirck in `async-graphql` subscription type names aren't
    /// as predictable as those for queries an mutations. This enables subscription sending,
    /// but behavior may be unpredictable
    pub fn with_unstable_subscriptions(mut self) -> Self {
        self.unstable_subscriptions = true;
        self
    }

    /// Determine how metadata should be determined. By default, none will be sent
    pub fn with_metadata<F>(mut self, func: F) -> Self
    where
        F: Fn(&ExtensionContext<'_>) -> Metadata + Send + Sync + 'static,
    {
        self.metadata = Some(Arc::new(func));
        self
    }
}

struct HiveUsageExtension {
    state: Mutex<HiveUsageState>,
    client: Arc<Mutex<HiveUsageBuffer>>,
    channel: mpsc::Sender<Report>,
    subscriptions: bool,
    metadata: Option<MetadataFn>,
}

#[derive(Default, PartialEq)]
enum SubscriptionState {
    #[default]
    NotSubscription,
    DetectedSubscription,
    SavedOperation,
}

#[derive(Default)]
struct HiveUsageState {
    hash_key: Option<String>,
    operation: Option<String>,
    fields: HashSet<String>,
    subscription: SubscriptionState,
}

impl ExtensionFactory for HiveUsageClient {
    fn create(&self) -> std::sync::Arc<dyn Extension> {
        Arc::new(HiveUsageExtension {
            state: Mutex::new(HiveUsageState::default()),
            client: self.buffer.clone(),
            channel: self.channel.clone(),
            subscriptions: self.unstable_subscriptions,

            metadata: self.metadata.clone(),
        })
    }
}

#[async_graphql::async_trait::async_trait]
impl Extension for HiveUsageExtension {
    fn subscribe<'s>(
        &self,
        ctx: &ExtensionContext<'_>,
        stream: BoxStream<'s, Response>,
        next: NextSubscribe<'_>,
    ) -> BoxStream<'s, Response> {
        if let Ok(mut state) = self.state.lock() {
            state.subscription = SubscriptionState::DetectedSubscription;
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
            let hash = BASE64_STANDARD.encode(xxh3_128(query.as_bytes()).to_ne_bytes());

            state.hash_key = Some(hash);
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

        if let Ok(mut state) = self.state.lock()
            && let Ok(mut buffer) = self.client.lock()
            && let Some(key) = &state.hash_key
            && let Some(operation) = &state.operation
            && !state.fields.is_empty()
            && !state.fields.contains("Query.__schema")
        {
            if !(buffer.map.contains_key(key)
                || !self.subscriptions && state.subscription != SubscriptionState::NotSubscription)
            {
                buffer.map.insert(
                    key.clone(),
                    OperationMapRecord {
                        operation: operation.clone(),
                        operation_name: operation_name.map(|e| e.to_string()),
                        fields: state.fields.iter().cloned().collect::<Vec<_>>(),
                    },
                );
            }

            if state.subscription == SubscriptionState::DetectedSubscription {
                if self.subscriptions {
                    let request = SubscriptionOperation {
                        timestamp: SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_millis(),
                        operation_map_key: key.clone(),
                        metadata: None,
                    };

                    buffer.subscriptions.push(request);
                    state.subscription = SubscriptionState::SavedOperation;
                }
            } else {
                let request = RequestOperation {
                    timestamp: SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_millis(),
                    operation_map_key: key.clone(),
                    metadata: self.metadata.clone().map(|f| f(ctx)),
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
        if !info.is_for_introspection
            && let Ok(mut state) = self.state.lock()
        {
            let parent_type = if self.subscriptions {
                if ctx.schema_env.registry.types.contains_key(info.parent_type) {
                    info.parent_type.to_string()
                } else {
                    ctx.schema_env
                        .registry
                        .subscription_type
                        .clone()
                        .unwrap_or("Subscription".to_string())
                }
            } else {
                info.parent_type.to_string()
            };

            state.fields.insert(parent_type.clone());

            state
                .fields
                .insert(format!("{}.{}", parent_type, info.name));
        }

        // dbg!(info);

        let result = next.run(ctx, info).await?;

        Ok(result)
    }

    async fn request(&self, ctx: &ExtensionContext<'_>, next: NextRequest<'_>) -> Response {
        let response = next.run(ctx).await;

        let buffer = match self.client.lock() {
            Ok(mut b) => b.clear(),
            Err(_) => return response,
        };

        let report: Report = buffer.into();

        let err = self.channel.clone().try_send(report);

        #[cfg(feature = "tracing")]
        {
            if let Err(err) = err {
                tracing::error!("{err}");
            }
        }

        response
    }
}
