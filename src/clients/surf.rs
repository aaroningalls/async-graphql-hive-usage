use uuid::Uuid;

use crate::types;

pub struct SurfClient {
    client: surf::Client,
}

impl SurfClient {
    pub fn new(client: surf::Client) -> Self {
        Self { client }
    }
}

impl Default for SurfClient {
    fn default() -> Self {
        Self {
            client: surf::Client::new(),
        }
    }
}

#[async_graphql::async_trait::async_trait]
impl crate::HiveSender for SurfClient {
    async fn send(&self, req: crate::HiveHTTPRequest, _: Uuid) -> Option<types::Response> {
        let mut post = self.client.post(req.url).body_bytes(req.body);

        for (key, value) in req.headers {
            post = post.header(key, value);
        }

        let res = post.send().await;

        #[cfg(feature = "tracing")]
        {
            if let Err(err) = &res {
                tracing::error!("request error: {err}")
            }
        }

        res.ok()?.body_json::<types::Response>().await.ok()
    }
}
