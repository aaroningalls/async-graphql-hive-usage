mod extension;
pub mod types;

use crate::extension::HiveUsageBuffer;
use async_graphql::{async_trait, extensions::ExtensionContext};
use futures::{StreamExt, channel::mpsc};
use std::sync::{Arc, Mutex};
use types::{Metadata, Report};
use uuid::Uuid;

pub mod clients;

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
///     async fn send(&self, req: HiveHTTPRequest, id: Uuid) -> Option<types::Response> {
///         // do stuff
///     }
/// }
/// ```
#[async_trait::async_trait]
pub trait HiveSender {
    /// Send the report
    async fn send(&self, req: HiveHTTPRequest, id: Uuid) -> Option<types::Response>;
}

type MetadataFn = Arc<dyn Fn(&ExtensionContext<'_>) -> Option<Metadata> + Send + Sync>;
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

async fn send_task<S: HiveSender>(
    report: Report,
    id: Uuid,
    sender: &S,
    target_id: Uuid,
    token: &str,
) {
    let body = match serde_json::to_vec(&report) {
        Ok(b) => b,
        Err(_) => return,
    };

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
            ("authorization", format!("Bearer {token}")),
            ("x-usage-api-version", "2".into()),
            ("x-request-id", id.to_string()),
            ("content-type", "application/json".into()),
        ],
        body,
    };

    let res = sender.send(req, id).await;

    #[cfg(feature = "tracing")]
    {
        if let Some(res) = res {
            match res {
                types::Response::Ok(b) => tracing::info!(
                    "hive request {}: {} accepted, {} rejected",
                    b.id,
                    b.operations.accepted,
                    b.operations.rejected
                ),
                types::Response::Err(b) => {
                    tracing::error!("{} hive errors: {:?}", b.errors.len(), b.errors)
                }
            }
        }
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
                    send_task(report, Uuid::new_v4(), &sender, target_id, &token).await;
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
    pub fn request_id<F>(mut self, func: F) -> Self
    where
        F: Fn() -> Uuid + Send + Sync + 'static,
    {
        self.request_id = Arc::new(func);
        self
    }

    /// Determine when the report should be sent, defaults to a report size of 1MB
    ///
    /// **This is called every GraphQL request.** It should be fairly quick to not impact clients
    pub fn should_send<F>(mut self, func: F) -> Self
    where
        F: Fn(&Report) -> bool + Send + Sync + 'static,
    {
        self.should_send = Some(Arc::new(func));
        self
    }

    /// Attach metadata to a GraphQL operation.
    pub fn metadata<F>(mut self, func: F) -> Self
    where
        F: Fn(&ExtensionContext<'_>) -> Option<Metadata> + Send + Sync + 'static,
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

                let id = (self.request_id)();

                if send {
                    send_task(report, id, &self.sender, self.target_id, &self.token).await;
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
