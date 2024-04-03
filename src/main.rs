#![feature(async_closure)]
mod config;
use std::{fs::File, io::Write, time::Duration};

use futures::{future::join_all, FutureExt, StreamExt};
use pulsar::{
    authentication::oauth2::{OAuth2Authentication, OAuth2Params},
    consumer::{ConsumerOptions, InitialPosition},
    proto::MessageIdData,
    reader::Reader,
    Authentication, Pulsar, TokioExecutor,
};
use serde::{Deserialize, Serialize};

static RECONNECT_DELAY: usize = 100; // wait 100 ms before trying to reconnect
static CHECK_CONNECTION_TIMEOUT: usize = 30_000;
static LOG_FREQUENCY: usize = 10;

pub async fn delay_ms(ms: usize) {
    tokio::time::sleep(Duration::from_millis(ms as u64)).await;
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Config {
    pub pulsar: PulsarConfig,
}

#[derive(Clone, Debug, Deserialize)]
pub struct PulsarConfig {
    pub hostname: String,
    pub port: u16,
    pub tenant: String,
    pub namespace: String,
    pub topic: String,
    pub token: Option<String>,
    pub oauth: Option<OAuth>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct OAuth {
    pub client_id: String,
    pub client_secret: String,
    pub client_email: String,
    pub issuer_url: String,
    pub audience: String,
}

async fn get_pulsar_client(config: Config) -> Result<Pulsar<TokioExecutor>, pulsar::Error> {
    let addr = format!(
        "pulsar+ssl://{}:{}",
        config.pulsar.hostname, config.pulsar.port
    );
    let mut builder = Pulsar::builder(addr, TokioExecutor);

    if let Some(token) = config.pulsar.token {
        let authentication = Authentication {
            name: "token".to_string(),
            data: token.into_bytes(),
        };
        builder = builder.with_auth(authentication);
    }

    if let Some(oauth) = config.pulsar.oauth {
        let credentials = serde_json::to_string(&oauth).unwrap();

        builder =
            builder.with_auth_provider(OAuth2Authentication::client_credentials(OAuth2Params {
                issuer_url: oauth.issuer_url.clone(),
                credentials_url: format!("data:application/json;,{}", credentials),
                audience: Some(oauth.audience),
                scope: None,
            }));
    }

    builder.build().await
}

async fn get_pulsar_reader(
    pulsar: Pulsar<TokioExecutor>,
    full_topic_name: &str,
    initial_position: Option<MessageIdData>,
) -> Result<Reader<String, TokioExecutor>, pulsar::Error> {
    pulsar
        .reader()
        .with_topic(full_topic_name)
        .with_consumer_name("test_reader")
        .with_options(
            if let Some(pos) = initial_position {
                log::warn!("Reconnecting reader starting from {:?}", pos);
                ConsumerOptions::default().starting_on_message(pos)
            } else {
                ConsumerOptions::default().with_initial_position(InitialPosition::Earliest)
            }
            .durable(false),
        )
        .into_reader()
        .await
}

async fn read_topic(pulsar: Pulsar<TokioExecutor>, namespace: String, topic: String) {
    let full_topic_name = format!("persistent://public/{}/{}", namespace, &topic);

    let filename = format!("data/{}.jsonl", &topic);
    let mut file = File::create(&filename).expect(&format!("Failed to create {}", &filename));

    let mut counter = 0_usize;
    let mut last_position: Option<MessageIdData> = None;
    loop {
        if let Ok(reader) =
            get_pulsar_reader(pulsar.clone(), &full_topic_name, last_position.clone()).await
        {
            let mut reader = reader.fuse();
            let check_connection_timer = delay_ms(CHECK_CONNECTION_TIMEOUT).fuse();
            futures::pin_mut!(check_connection_timer);

            'inner: loop {
                futures::select! {
                        // This is necessary due to a bug in our Pulsar broker
                        // When the broker recovers the offloaded data from s3, it sometimes fails and hangs
                        _ = check_connection_timer => {
                            let connection_result = reader.get_mut().check_connection().await;

                            if let Err(e) = connection_result {
                                log::error!("Check connection failed, attempting to reconnect... {}", e.to_string());
                            }
                            break 'inner;
                        }

                        reader_message = reader.next() => {
                            if let Some(msg) = reader_message {
                                check_connection_timer.set(delay_ms(CHECK_CONNECTION_TIMEOUT).fuse());

                                let msg = msg.expect("Failed to read message");
                                let message_id = msg.message_id();

                                // Necessary to skip repeats due to reconnecting from last position
                                if last_position == Some(message_id.clone()) {
                                    log::warn!("Skipping repeated message");
                                    continue;
                                }
                                last_position = Some(message_id.clone());

                                file.write(&msg.payload.data).expect("Failed to write data");
                                file.write(b"\n").expect("Failed to write delimiter");

                                counter += 1;
                                if counter % LOG_FREQUENCY == 0 {
                                    log::info!("{} got {} messages", &topic, counter);
                                }
                            }
                        }
                }
            }
        } else {
            delay_ms(RECONNECT_DELAY).await;
            log::error!(
                "Failed to create reader on {} . Retrying...",
                &full_topic_name
            );
        }
    }
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let config: Config = config::load().expect("Unable to load config");
    let namespace = config.pulsar.namespace.clone();
    let topic = config.pulsar.topic.clone();
    let pulsar_client = get_pulsar_client(config)
        .await
        .expect("Failed to build pulsar client");

    let topics = vec![topic];

    let readers = topics
        .into_iter()
        .map(|topic| read_topic(pulsar_client.clone(), namespace.clone(), topic))
        .collect::<Vec<_>>();
    join_all(readers).await;
}
