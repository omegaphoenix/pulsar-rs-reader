#![feature(async_closure)]
mod config;
use futures::future::join_all;
use futures::stream::Fuse;
use futures::FutureExt;
use futures::StreamExt;
use pulsar::{
    consumer::{ConsumerOptions, InitialPosition},
    proto::MessageIdData,
    reader::Reader,
    Authentication, Pulsar, TokioExecutor,
};
use serde::Deserialize;
use std::time::Duration;
use std::{fs::File, io::Write};

pub async fn delay_ms(ms: usize) {
    tokio::time::sleep(Duration::from_millis(ms as u64)).await;
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Config {
    pub pulsar: PulsarConfig,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct PulsarConfig {
    pub hostname: String,
    pub port: u16,
    pub tenant: String,
    pub namespace: String,
    pub token: Option<String>,
}

async fn get_pulsar_client(config: Config) -> Pulsar<TokioExecutor> {
    let addr = format!(
        "pulsar+ssl://{}:{}",
        config.pulsar.hostname, config.pulsar.port
    );
    let mut builder = Pulsar::builder(addr, TokioExecutor);

    let authentication = Authentication {
        name: "token".to_string(),
        data: config.pulsar.token.unwrap().into_bytes(),
    };

    builder = builder.with_auth(authentication);

    builder.build().await.expect("Failed to build")
}

async fn read_topic(pulsar: Pulsar<TokioExecutor>, namespace: String, topic: String) {
    let full_topic_name = format!("persistent://public/{}/{}", namespace, &topic);

    let filename = format!("data/{}.jsonl", &topic);
    let mut file = File::create(&filename).expect(&format!("Failed to create {}", &filename));

    let mut counter = 0_usize;
    let mut initial_position: Option<MessageIdData> = None;
    loop {
        let mut reader: Fuse<Reader<String, _>> = pulsar
            .reader()
            .with_topic(&full_topic_name)
            .with_consumer_name("test_reader")
            .with_options(
                if let Some(pos) = initial_position.clone() {
                    ConsumerOptions::default().starting_on_message(pos.clone())
                } else {
                    ConsumerOptions::default().with_initial_position(InitialPosition::Earliest)
                }
                .durable(false),
            )
            .into_reader()
            .await
            .expect(&format!("Failed to create reader {}", &topic))
            .fuse();

        let check_connection_timeout = 30_000;
        let check_connection_timer = delay_ms(check_connection_timeout).fuse();
        futures::pin_mut!(check_connection_timer);

        'inner: loop {
            futures::select! {
                    _ = check_connection_timer => {
                        let connection_result = reader.get_mut().check_connection().await;

                        if let Err(e) = connection_result {
                            log::error!("Check connection failed, attempting to reconnect... {}", e.to_string());
                        }
                        break 'inner;
                    }

                    reader_message = reader.next() => {
                        if let Some(msg) = reader_message {
                            check_connection_timer.set(delay_ms(check_connection_timeout).fuse());
                            let msg = msg.expect("Failed to read message");
                            file.write(&msg.payload.data).expect("Failed to write data");
                            file.write(b"\n").expect("Failed to write delimiter");
                            let message_id = msg.message_id();
                            initial_position = Some(message_id.clone());

                            counter += 1;
                            if counter % 10 == 0 {
                                log::info!("{} got {} messages", &topic, counter);
                            }
                        }
                    }
            }
        }
    }
}

fn get_topic(game_id: &str) -> String {
    format!("flex_cv_tracks_{}", game_id)
}

#[tokio::main]
async fn main() -> Result<(), pulsar::Error> {
    env_logger::init();
    let config: Config = config::load().expect("Unable to load config");
    let namespace = config.pulsar.namespace.clone();

    let game_ids = vec!["3b4581f9-0cc1-4a3b-a6cf-f02d816b7473"];
    let topics = game_ids
        .into_iter()
        .map(|game_id| get_topic(&game_id))
        .collect::<Vec<_>>();
    let pulsar_client = get_pulsar_client(config).await;
    let readers = topics
        .into_iter()
        .map(|topic| read_topic(pulsar_client.clone(), namespace.clone(), topic))
        .collect::<Vec<_>>();
    join_all(readers).await;
    Ok(())
}
