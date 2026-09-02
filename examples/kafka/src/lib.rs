//! `examples/kafka`: a self-contained Kafka starter demo — an `OrderProducer`
//! component publishes an `Order` on startup, and an `#[kafka_listeners]`
//! component consumes it (exercising panic-safe at-least-once delivery, retry
//! and the `orders.DLT` dead-letter topic).

// Force oxide-kafka to be linked (inventory registrations for the listener
// descriptors + kafka health indicator).
#[allow(unused_imports)]
use oxide_kafka as _;

use oxide_boot::component;
use oxide_core::ComponentResult;
use oxide_kafka::{kafka_listeners, KafkaProducer, MessageKey};
use serde::{Deserialize, Serialize};

/// The business payload flowing through Kafka.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Order {
    pub id: u32,
    pub customer: String,
    pub amount: f64,
}

/// Publishes a welcome `Order` on boot to show the producer side.
///
/// `post_construct` runs once the DI graph is up, so the `KafkaProducer` is
/// already connected; it then publishes to the `orders` topic.
#[component(post_construct = publish)]
pub struct OrderProducer {
    producer: KafkaProducer,
}

impl OrderProducer {
    async fn publish(&self) -> ComponentResult<()> {
        let order = Order { id: 1, customer: "oxide".to_string(), amount: 42.0 };
        self.producer
            .send("orders", &order.id.to_string(), &order)
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
        tracing::info!(id = order.id, "published order to `orders`");
        Ok(())
    }
}

/// Consumes `orders` with `#[kafka_listeners]`.
#[component]
pub struct OrderListener {
    #[default]
    _marker: bool,
}

#[kafka_listeners]
impl OrderListener {
    #[kafka_listener(topic = "orders", group_id = "orders-demo")]
    async fn on_order(&self, order: Order, key: MessageKey) -> Result<(), String> {
        tracing::info!(id = order.id, key = ?key.0, customer = %order.customer, amount = order.amount, "received order");
        Ok(())
    }
}