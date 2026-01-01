use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fmt::Display};
use uuid::Uuid;

#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
/// The serializable report that Hive expects. This should be sent as JSON to your
/// hive endpoint. See https://the-guild.dev/graphql/hive/docs/api-reference/usage-reports
pub struct Report {
    pub size: i32,
    pub map: HashMap<String, OperationMapRecord>,
    pub operations: Option<Vec<RequestOperation>>,
    pub subscription_operations: Option<Vec<SubscriptionOperation>>,
}

#[derive(Serialize, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct OperationMapRecord {
    pub operation: String,
    pub operation_name: Option<String>,
    pub fields: Vec<String>,
}

#[derive(Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct RequestOperation {
    pub timestamp: u128,
    pub operation_map_key: String,
    pub execution: Execution,
    pub metadata: Option<Metadata>,
    pub persisted_document_hash: Option<String>,
}

#[derive(Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Execution {
    pub ok: bool,
    pub duration: u128,
    pub errors_total: i32,
}

#[derive(Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SubscriptionOperation {
    pub timestamp: u128,
    pub operation_map_key: String,
    pub metadata: Option<Metadata>,
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

#[derive(Deserialize, Debug, Clone)]
#[serde(untagged)]
pub enum Response {
    Ok(OkResponse),
    Err(ErrResponse),
}

#[derive(Deserialize, Debug, Clone)]
pub struct OkResponse {
    pub id: Uuid,
    pub operations: OperationStatus,
}
#[derive(Deserialize, Debug, Clone)]
pub struct OperationStatus {
    pub accepted: i32,
    pub rejected: i32,
}

#[derive(Deserialize, Debug, Clone)]
pub struct ErrResponse {
    pub errors: Vec<OperationError>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct OperationErrors {
    pub message: String,
    pub path: String,
    pub errors: Vec<OperationError>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct OperationError {
    pub message: String,
    pub path: String,
}

impl Display for OperationErrors {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl Display for OperationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}
