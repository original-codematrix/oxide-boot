use oxide_kafka::kafka_listeners;

struct Wrong;

#[kafka_listeners]
impl Wrong {
    #[kafka_listener(topic = "x")]
    async fn no_self(payload: String) {}
}

fn main() {}