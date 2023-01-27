# pulsar-rs reader example

Example for using the pulsar-rs crate to read from a topic

This example also includes reconnect logic to get around a known bug in the Pulsar broker where recovering offloaded data from S3 sometimes causes fetching the next message to hang indefinitely. The reconnect code can be removed if you aren't experiencing this bug in your broker.

## To Run
1. `cp config.sample.toml config.toml`
1. Fill in the pulsar hostname, port, tenant, namespace, topic, and token in the `config.toml`
1. `RUST_LOG=info cargo run`
1. Note that the `cargo run` will hang indefinitely in case more messages are sent to the Pulsar topic
1. Data will be written to `data/{topic_name}.jsonl`
