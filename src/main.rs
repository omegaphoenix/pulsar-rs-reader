#![feature(async_closure)]
mod config;
use futures::future::join_all;
use futures::StreamExt;
use pulsar::{
    consumer::{ConsumerOptions, InitialPosition},
    reader::Reader,
    Authentication, Pulsar, TokioExecutor,
};
use serde::Deserialize;
use std::{fs::File, io::Write};

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

    let mut reader: Reader<String, _> = pulsar
        .reader()
        .with_topic(&full_topic_name)
        .with_consumer_name("test_reader")
        .with_options(
            ConsumerOptions::default()
                .with_initial_position(InitialPosition::Earliest)
                .durable(false),
        )
        .into_reader()
        .await
        .expect(&format!("Failed to create reader {}", &topic));

    let mut counter = 0_usize;
    while let Some(msg) = reader.next().await {
        let msg = msg.expect("Failed to read message");
        file.write(&msg.payload.data).expect("Failed to write data");
        file.write(b"\n").expect("Failed to write delimiter");

        counter += 1;
        if counter % 10 == 0 {
            log::info!("{} got {} messages", &topic, counter);
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
