use crate::types;

pub struct UreqClient {
    client: ureq::Agent,
}

impl UreqClient {
    pub fn new(agent: ureq::Agent) -> Self {
        Self { client: agent }
    }
}

impl Default for UreqClient {
    fn default() -> Self {
        Self {
            client: ureq::Agent::new_with_defaults(),
        }
    }
}

#[async_graphql::async_trait::async_trait]
impl crate::HiveSender for UreqClient {
    async fn send(&self, req: crate::HiveHTTPRequest, _: uuid::Uuid) -> Option<types::Response> {
        let mut post = self.client.post(req.url);

        for (key, value) in req.headers {
            post = post.header(key, value);
        }

        let res = post.send(req.body);

        #[cfg(feature = "tracing")]
        {
            if let Err(err) = &res {
                tracing::error!("request error: {err}")
            }
        }

        res.ok()?.into_body().read_json::<types::Response>().ok()
    }
}
