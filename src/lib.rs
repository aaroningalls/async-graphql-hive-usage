use async_graphql::{
    Response, ServerResult, Value, Variables, async_trait,
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
    /// Authorization, API version, and request_id headers
    pub headers: Vec<(&'static str, String)>,
    /// Usage report serialized as JSON
    pub body: Vec<u8>,
}

/// Used to send the usage report to hive. Implement the async trait
/// send to determine how to send the report to Hive (which client, error handling, etc).
/// ```rs
/// // async-graphql exports async_trait
/// #[async_trait::async_trait]
/// impl HiveSender for MyHive {
///     async fn send(&self, req: HiveHTTPRequest) {
///         // do stuff
///     }
/// }
/// ```
#[async_trait::async_trait]
pub trait HiveSender {
    /// Send the report
    async fn send(&self, req: HiveHTTPRequest);
}

type MetadataFn = Arc<dyn Fn(&ExtensionContext<'_>) -> Metadata + Send + Sync>;
type RequestIdFn = Arc<dyn Fn() -> Uuid + Send + Sync>;
type ShouldSendFn = Arc<dyn Fn(&Report) -> bool + Send + Sync>;

/// The client is passed to your async-graphql schema builder as an extension.
/// This struct stores the buffered requests until they are sent and cleared
pub struct HiveUsageClient {
    buffer: Arc<Mutex<HiveUsageBuffer>>,
    channel: mpsc::Sender<(Report, usize)>,
    unstable_subscriptions: bool,
    metadata: Option<MetadataFn>,
}

pub struct HiveClientBuilder<S: HiveSender> {
    target_id: Uuid,
    token: String,
    sender: S,
    request_id: RequestIdFn,
    should_send: Option<ShouldSendFn>,
    metadata: Option<MetadataFn>,
    send_size: usize,
}

#[derive(Default, Debug)]
struct HiveUsageBuffer {
    map: HashMap<String, OperationMapRecord>,
    operations: Vec<RequestOperation>,
    subscriptions: Vec<SubscriptionOperation>,
    approx_size: usize,
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
    pub fn new(
        sender: impl HiveSender,
        target_id: Uuid,
        token: String,
    ) -> (Self, impl Future<Output = ()>) {
        let (send, mut recv) = mpsc::channel::<(Report, usize)>(3);

        let task = async move {
            while let Some((report, size)) = recv.next().await {
                if size > 5_000_000 {
                    let body = match serde_json::to_vec(&report) {
                        Ok(b) => b,
                        Err(_) => continue,
                    };

                    let id = Uuid::new_v4().to_string();

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

    /// Customize the behavior of the extension
    /// - `send_size` is approximately how big the report should be when sent
    /// - `should_send` determines if the report should be sent, overriding size
    /// - `request_id` determines how the request UUID should be generated
    /// - `metadata` configures the metadata for a graphql request
    pub fn builder<S: HiveSender>(
        sender: S,
        target_id: Uuid,
        token: String,
    ) -> HiveClientBuilder<S> {
        HiveClientBuilder {
            target_id,
            token,
            sender,
            request_id: Arc::new(Uuid::new_v4),
            metadata: None,
            should_send: None,
            send_size: 5_000_000,
        }
    }

    /// Because of a quirk in `async-graphql` subscription type names aren't
    /// as predictable as those for queries an mutations. This enables subscription sending,
    /// but behavior may be unpredictable
    pub fn with_unstable_subscriptions(mut self) -> Self {
        self.unstable_subscriptions = true;
        self
    }
}

impl<S: HiveSender> HiveClientBuilder<S> {
    /// Generate a UUID to be send, unique to the usage report request
    pub fn with_request_id<F>(mut self, func: F) -> Self
    where
        F: Fn() -> Uuid + Send + Sync + 'static,
    {
        self.request_id = Arc::new(func);
        self
    }

    /// Determine when the report should be sent, defaults to a report size of 1MB
    ///
    /// **This is called every GraphQL request.** It should be fairly quick to not impact clients
    pub fn with_should_send<F>(mut self, func: F) -> Self
    where
        F: Fn(&Report) -> bool + Send + Sync + 'static,
    {
        self.should_send = Some(Arc::new(func));
        self
    }

    /// Attach metadata to a GraphQL operation.
    pub fn with_metadata<F>(mut self, func: F) -> Self
    where
        F: Fn(&ExtensionContext<'_>) -> Metadata + Send + Sync + 'static,
    {
        self.metadata = Some(Arc::new(func));
        self
    }

    /// How big the report should be when sent. This value is ignored if a `should_send` function is set. Defaults to ~5MB
    pub fn send_size(mut self, size: usize) -> Self {
        self.send_size = size;
        self
    }

    pub fn build(self) -> (HiveUsageClient, impl Future<Output = ()>) {
        let (send, mut recv) = mpsc::channel::<(Report, usize)>(3);

        let task = async move {
            while let Some((report, size)) = recv.next().await {
                let send = if let Some(ref f) = self.should_send {
                    (f)(&report)
                } else {
                    size > self.send_size
                };

                if send {
                    let body = match serde_json::to_vec(&report) {
                        Ok(b) => b,
                        Err(_) => continue,
                    };

                    let id = (self.request_id)().to_string();

                    #[cfg(feature = "tracing")]
                    {
                        tracing::info!(
                            "Hive Usage ({id}): sending {} operations",
                            report.operations.map(|e| e.len()).unwrap_or(0)
                        )
                    }

                    let req = HiveHTTPRequest {
                        url: format!("https://app.graphql-hive.com/usage/{}", self.target_id),
                        headers: vec![
                            ("Authorization", format!("Bearer {}", self.token)),
                            ("X-Usage-API-Version", "2".into()),
                            ("X-Request-Id", id),
                            ("Content-Type", "application/json".into()),
                        ],
                        body,
                    };

                    self.sender.send(req).await;
                }
            }
        };

        (
            HiveUsageClient {
                channel: send,
                buffer: Arc::new(Mutex::new(HiveUsageBuffer::default())),
                unstable_subscriptions: false,
                metadata: self.metadata,
            },
            task,
        )
    }
}

struct HiveUsageExtension {
    state: Mutex<HiveUsageState>,
    client: Arc<Mutex<HiveUsageBuffer>>,
    channel: mpsc::Sender<(Report, usize)>,
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
            let mut size = 0;
            if !(buffer.map.contains_key(key)
                || !self.subscriptions && state.subscription != SubscriptionState::NotSubscription)
            {
                size += operation.len() + key.len();
                let record = OperationMapRecord {
                    operation: operation.clone(),
                    operation_name: operation_name.map(|e| {
                        size += e.len() + 2;
                        e.to_string()
                    }),
                    fields: state
                        .fields
                        .iter()
                        .inspect(|e| size += e.len() + 2)
                        .cloned()
                        .collect::<Vec<_>>(),
                };

                buffer.map.insert(key.clone(), record);
            }

            if state.subscription == SubscriptionState::DetectedSubscription {
                if self.subscriptions {
                    let timestamp = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_millis();

                    size += key.len() + 2 + (timestamp.ilog10() + 1) as usize;

                    let request = SubscriptionOperation {
                        timestamp,
                        operation_map_key: key.clone(),
                        metadata: None,
                    };

                    buffer.subscriptions.push(request);
                    state.subscription = SubscriptionState::SavedOperation;
                }
            } else {
                let timestamp = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis();

                let duration = duration.as_nanos();
                let errors_total = response.errors.len();

                size += key.len()
                    + 2
                    + errors_total
                    + (timestamp.ilog10() + duration.ilog10() + 2) as usize;

                let request = RequestOperation {
                    timestamp,
                    operation_map_key: key.clone(),
                    metadata: self.metadata.clone().map(|f| {
                        let m = f(ctx);

                        m.client
                            .as_ref()
                            .inspect(|e| size += e.name.len() + e.version.len() + 4);
                        m
                    }),
                    persisted_document_hash: None,
                    execution: Execution {
                        ok: response.is_ok(),
                        duration,
                        errors_total: errors_total as i32,
                    },
                };

                buffer.operations.push(request);
            }
            buffer.approx_size += size;
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
        let size = buffer.approx_size;
        let report: Report = buffer.into();

        let err = self.channel.clone().try_send((report, size));

        #[cfg(feature = "tracing")]
        {
            if let Err(err) = err {
                tracing::error!("{err}");
            }
        }

        response
    }
}
