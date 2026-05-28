use std::sync::Arc;

use crate::content::ApObjectHandler;
use crate::repository::FederationRepository;
use crate::user::ApUserRepository;

/// Typed event emitted by the federation layer. Consumers wire in an
/// [`EventPublisher`] to receive these and drive side effects (job queues,
/// webhooks, metrics, etc.).
///
/// # Delivery flow
///
/// When an `EventPublisher` is configured, outbound activities are NOT
/// delivered directly — instead a [`FederationEvent::DeliveryRequested`] event
/// is published for each target inbox. The consumer's job queue should:
/// 1. Persist the event.
/// 2. Call [`crate::service::ActivityPubService::deliver_to_inbox`] when
///    processing the queue item.
///
/// Without a publisher, the library falls back to fire-and-forget
/// `tokio::spawn` delivery (no persistence across restarts).
#[derive(Debug, Clone)]
pub enum FederationEvent {
    /// An outbound activity must be delivered to `inbox`.
    /// Call `ActivityPubService::deliver_to_inbox(inbox, activity, signing_actor_id)`.
    DeliveryRequested {
        inbox: url::Url,
        activity: serde_json::Value,
        signing_actor_id: uuid::Uuid,
    },
    /// Delivery to `inbox` failed permanently after all in-process retries.
    /// The consumer may schedule additional retries or alert.
    DeliveryFailed {
        inbox: url::Url,
        activity: serde_json::Value,
        signing_actor_id: uuid::Uuid,
        error: String,
    },
}

/// Receives typed federation events from the library.
///
/// Implement this trait to bridge federation events into your application's
/// job queue, message broker, or metrics system.
#[async_trait::async_trait]
pub trait EventPublisher: Send + Sync {
    async fn publish(&self, event: FederationEvent) -> anyhow::Result<()>;
}

#[derive(Clone)]
pub struct FederationData {
    pub(crate) federation_repo: Arc<dyn FederationRepository>,
    pub(crate) user_repo: Arc<dyn ApUserRepository>,
    pub(crate) object_handler: Arc<dyn ApObjectHandler>,
    pub(crate) base_url: String,
    pub(crate) domain: String,
    pub(crate) allow_registration: bool,
    pub(crate) software_name: String,
    pub(crate) event_publisher: Option<Arc<dyn EventPublisher>>,
}

impl FederationData {
    pub fn new(
        federation_repo: Arc<dyn FederationRepository>,
        user_repo: Arc<dyn ApUserRepository>,
        object_handler: Arc<dyn ApObjectHandler>,
        base_url: String,
        allow_registration: bool,
        software_name: String,
        event_publisher: Option<Arc<dyn EventPublisher>>,
    ) -> Self {
        let domain = base_url
            .trim_start_matches("https://")
            .trim_start_matches("http://")
            .split('/')
            .next()
            .unwrap_or("")
            .to_string();
        Self {
            federation_repo,
            user_repo,
            object_handler,
            base_url,
            domain,
            allow_registration,
            software_name,
            event_publisher,
        }
    }
}
