use base64::{Engine, prelude::BASE64_STANDARD};
use futures::{channel::mpsc, stream::BoxStream};
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::{Instant, SystemTime, UNIX_EPOCH},
};
use xxhash_rust::xxh3::xxh3_128;

use crate::{
    HiveUsageClient,
    types::{Execution, OperationMapRecord, Report, RequestOperation, SubscriptionOperation},
};
use async_graphql::{
    Response, ServerResult, Value, Variables,
    extensions::{
        Extension, ExtensionContext, ExtensionFactory, NextExecute, NextParseQuery, NextRequest,
        NextResolve, NextSubscribe, ResolveInfo,
    },
    parser::types::ExecutableDocument,
};

struct HiveUsageExtension {
    state: Mutex<HiveUsageState>,
    client: Arc<Mutex<HiveUsageBuffer>>,
    channel: mpsc::Sender<(Report, usize)>,
    subscriptions: bool,
    metadata: Option<crate::MetadataFn>,
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

#[derive(Default, Debug)]
pub struct HiveUsageBuffer {
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
                    metadata: self.metadata.as_ref().and_then(|f| {
                        let m = f(ctx);

                        m.inspect(|e| {
                            e.client
                                .as_ref()
                                .inspect(|e| size += e.name.len() + e.version.len() + 4);
                        })
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
