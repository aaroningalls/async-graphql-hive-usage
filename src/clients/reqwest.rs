use reqwest::header::{HeaderMap, HeaderName, HeaderValue};

/// A reqwest client for sending to hive. ReqestClient.send can be
/// passed as the implementation for HiveSender
pub struct ReqwestClient {
    client: reqwest::Client,
}

impl ReqwestClient {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }

    pub fn send(&self, report: crate::HiveHTTPRequest) -> impl Future {
        let headers = HeaderMap::from_iter(report.headers.iter().filter_map(|(k, v)| {
            HeaderValue::from_str(v)
                .map(|e| (HeaderName::from_static(k), e))
                .ok()
        }));

        self.client
            .post(report.url)
            .headers(headers)
            .body(report.body)
            .send()
    }
}

impl Default for ReqwestClient {
    fn default() -> Self {
        Self::new()
    }
}
