use std::sync::Arc;

use crate::content::{ApContentReader, ApObjectHandler};
use crate::repository::{
    ActivityRepository, ActorRepository, BlocklistRepository, FollowRepository,
};
use crate::user::ApUserRepository;

/// Typed event emitted by the federation layer.
///
/// When an [`EventPublisher`] is configured, outbound activities are NOT
/// delivered directly — instead a [`FederationEvent::DeliveryRequested`] event
/// is published per inbox. The consumer's job queue should:
/// 1. Persist the event.
/// 2. Call [`crate::service::ActivityPubService::deliver_to_inbox`] when processing.
///
/// Without a publisher, the library falls back to `tokio::spawn` delivery.
#[derive(Debug, Clone)]
pub enum FederationEvent {
    DeliveryRequested {
        inbox: url::Url,
        activity: serde_json::Value,
        signing_actor_id: uuid::Uuid,
    },
    DeliveryFailed {
        inbox: url::Url,
        activity: serde_json::Value,
        signing_actor_id: uuid::Uuid,
        error: String,
    },
}

/// Receives typed federation events.
#[async_trait::async_trait]
pub trait EventPublisher: Send + Sync {
    async fn publish(&self, event: FederationEvent) -> anyhow::Result<()>;
}

#[derive(Clone)]
pub struct FederationData {
    pub(crate) activity_repo: Arc<dyn ActivityRepository>,
    pub(crate) follow_repo: Arc<dyn FollowRepository>,
    pub(crate) actor_repo: Arc<dyn ActorRepository>,
    pub(crate) blocklist_repo: Arc<dyn BlocklistRepository>,
    pub(crate) user_repo: Arc<dyn ApUserRepository>,
    pub(crate) content_reader: Arc<dyn ApContentReader>,
    pub(crate) object_handler: Arc<dyn ApObjectHandler>,
    pub(crate) base_url: String,
    pub(crate) domain: String,
    pub(crate) allow_registration: bool,
    pub(crate) software_name: String,
    pub(crate) event_publisher: Option<Arc<dyn EventPublisher>>,
}

impl FederationData {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        activity_repo: Arc<dyn ActivityRepository>,
        follow_repo: Arc<dyn FollowRepository>,
        actor_repo: Arc<dyn ActorRepository>,
        blocklist_repo: Arc<dyn BlocklistRepository>,
        user_repo: Arc<dyn ApUserRepository>,
        content_reader: Arc<dyn ApContentReader>,
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
            activity_repo,
            follow_repo,
            actor_repo,
            blocklist_repo,
            user_repo,
            content_reader,
            object_handler,
            base_url,
            domain,
            allow_registration,
            software_name,
            event_publisher,
        }
    }
}
