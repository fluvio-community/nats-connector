use anyhow::{Context, Result};
use async_std::channel::{self, Sender};
use async_std::task::spawn;
use async_trait::async_trait;
use fluvio::Offset;
use fluvio_connector_common::{
    tracing::{info, trace},
    Source,
};
use futures::{stream::LocalBoxStream, StreamExt};
use url::Url;

use crate::{config::NatsConfig, NatsEvent};

const CHANNEL_BUFFER_SIZE: usize = 10000;

pub(crate) struct NatsSource {
    server: String,
    subject: String,
}

impl NatsSource {
    pub(crate) fn new(config: &NatsConfig) -> Result<Self> {
        let url = Url::parse(&config.host).context("unable to parse NATS endpoint url")?;
        let subject = config.subject.clone();
        Ok(Self {
            server: url.to_string(),
            subject,
        })
    }
}

#[async_trait]
impl<'a> Source<'a, String> for NatsSource {
    async fn connect(self, _offset: Option<Offset>) -> Result<LocalBoxStream<'a, String>> {
        info!("Nats host: {} subject {}", &self.server, &self.subject);

        let (sender, receiver) = channel::bounded(CHANNEL_BUFFER_SIZE);
        spawn(nats_loop(sender, self.server, self.subject));
        Ok(receiver.boxed_local())
    }
}

async fn nats_loop(tx: Sender<String>, nats_host: String, nats_subject: String) -> Result<()> {
    info!("Nats loop started");

    loop {
        let nats_client = async_nats::connect(nats_host.clone()).await?;
        let mut nats_subscription = nats_client.subscribe(nats_subject.clone()).await?;

        info!("Nats connnecting to server {:?}", nats_client.server_info());

        while let Some(msg) = nats_subscription.next().await {
            trace!("Nats got: {msg:?}");
            let nats_event: NatsEvent = msg.into();
            tx.send(nats_event.try_into()?).await?;
        }
    }
}
