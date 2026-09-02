use oxide_kafka::kafka_listeners;

#[kafka_listeners]
async fn not_an_impl() {}

fn main() {}