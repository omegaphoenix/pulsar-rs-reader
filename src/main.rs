#![feature(async_closure)]
mod config;
use futures::future::join_all;
use serde::Deserialize;

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

async fn read_topic(topic: String) {
    let config: Config = config::load().expect("Unable to load config");
}

fn get_topic(game_id: &str) -> String {
    format!("flex_cv_tracks_{}", game_id)
}

#[tokio::main]
async fn main() -> Result<(), pulsar::Error> {
    env_logger::init();

    let game_ids = vec!["3b4581f9-0cc1-4a3b-a6cf-f02d816b7473"];
    let topics = game_ids
        .into_iter()
        .map(|game_id| get_topic(&game_id))
        .collect::<Vec<_>>();
    let readers = topics
        .into_iter()
        .map(|topic| read_topic(topic))
        .collect::<Vec<_>>();
    join_all(readers).await;
    Ok(())
}
