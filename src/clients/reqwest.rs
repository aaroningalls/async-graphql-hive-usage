use reqwest::header::{HeaderMap, HeaderName, HeaderValue};

use crate::{HiveSender, types};

/// A reqwest client for sending to hive. Implements `HiveSender`
/// ```rs
/// HiveUsageClient::new(ReqwestClient::default(), target_id, token);
/// //or
/// HiveUsageClient::new(ReqwestClient::new(client), target_id, token);
///
/// ```
pub struct ReqwestClient {
    client: reqwest::Client,
}

impl ReqwestClient {
    pub fn new(client: reqwest::Client) -> Self {
        Self { client }
    }
}

impl Default for ReqwestClient {
    fn default() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }
}

#[async_graphql::async_trait::async_trait]
impl HiveSender for ReqwestClient {
    async fn send(&self, req: crate::HiveHTTPRequest, id: uuid::Uuid) -> Option<types::Response> {
        let headers = req.header_map();

        let res = self
            .client
            .post(req.url)
            .headers(headers)
            .body(req.body)
            .send()
            .await;

        #[cfg(feature = "tracing")]
        {
            if let Err(err) = &res {
                tracing::error!("request error: {err}")
            }
        }
        res.ok()?.json::<types::Response>().await.ok()
    }
}

impl crate::HiveHTTPRequest {
    pub fn header_map(&self) -> HeaderMap {
        HeaderMap::from_iter(self.headers.iter().filter_map(|(k, v)| {
            HeaderValue::from_str(v)
                .map(|e| (HeaderName::from_static(k), e))
                .ok()
        }))
    }
}
