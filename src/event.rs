use async_nats::{Message as NatsMessage, Subject};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct NatsEvent {
    pub nats_subject: Subject,
    pub nats_reply: Option<Subject>,
    pub nats_data: Vec<u8>,
}

#[derive(Error, Debug)]
pub enum NatsEventError {
    #[error("Internal failure to convert Nats Event to JSON String: {0}")]
    InternalConversion(String),
}

impl From<NatsMessage> for NatsEvent {
    fn from(msg: NatsMessage) -> Self {
        Self {
            nats_subject: msg.subject,
            nats_reply: msg.reply,
            nats_data: msg.payload.into(),
        }
    }
}

impl TryFrom<NatsEvent> for String {
    type Error = NatsEventError;
    fn try_from(value: NatsEvent) -> Result<Self, Self::Error> {
        serde_json::to_string(&value).map_err(|e| NatsEventError::InternalConversion(e.to_string()))
    }
}
