mod config;
mod event;
mod source;

use config::NatsConfig;

use event::NatsEvent;
use fluvio::{RecordKey, TopicProducerPool};
use fluvio_connector_common::{
    connector,
    tracing::{debug, trace},
    Result, Source,
};
use futures::StreamExt;
use source::NatsSource;

#[connector(source)]
async fn start(config: NatsConfig, producer: TopicProducerPool) -> Result<()> {
    debug!(?config);
    let source = NatsSource::new(&config)?;
    let mut stream = source.connect(None).await?;
    while let Some(item) = stream.next().await {
        trace!(?item);
        producer.send(RecordKey::NULL, item).await?;
    }
    Ok(())
}
