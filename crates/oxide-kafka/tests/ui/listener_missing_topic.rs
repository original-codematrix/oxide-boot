use oxide_kafka::kafka_listeners;

struct Wrong;

#[kafka_listeners]
impl Wrong {
    #[kafka_listener]
    async fn missing_topic(&self, payload: String) {}
}

fn main() {}