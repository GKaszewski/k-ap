use std::sync::Arc;

use crate::{
    content::{ApContentReader, ApObjectHandler},
    data::FederationData,
    federation::ApFederationConfig,
    repository::{ActivityRepository, ActorRepository, BlocklistRepository, FollowRepository},
    url_scheme::{DefaultUrlScheme, UrlScheme},
    user::ApUserRepository,
};

use super::{
    ACTOR_CACHE_TTL_SECS, ActivityPubService, DELIVERY_INITIAL_DELAY_SECS, DELIVERY_MAX_ATTEMPTS,
};

pub struct ActivityPubServiceBuilder {
    activity_repo: Option<Arc<dyn ActivityRepository>>,
    follow_repo: Option<Arc<dyn FollowRepository>>,
    actor_repo: Option<Arc<dyn ActorRepository>>,
    blocklist_repo: Option<Arc<dyn BlocklistRepository>>,
    user_repo: Option<Arc<dyn ApUserRepository>>,
    content_reader: Option<Arc<dyn ApContentReader>>,
    object_handler: Option<Arc<dyn ApObjectHandler>>,
    base_url: String,
    allow_registration: bool,
    software_name: String,
    debug: bool,
    event_publisher: Option<Arc<dyn crate::data::EventPublisher>>,
    delivery_max_attempts: u32,
    delivery_initial_delay_secs: u64,
    signed_fetch_actor_id: Option<uuid::Uuid>,
    actor_cache_ttl_secs: u64,
    url_scheme: Option<Arc<dyn UrlScheme>>,
    nodeinfo_services_inbound: Vec<String>,
    nodeinfo_services_outbound: Vec<String>,
    nodeinfo_metadata: serde_json::Value,
}

impl ActivityPubServiceBuilder {
    pub(super) fn new(base_url: String) -> Self {
        Self {
            activity_repo: None,
            follow_repo: None,
            actor_repo: None,
            blocklist_repo: None,
            user_repo: None,
            content_reader: None,
            object_handler: None,
            base_url,
            allow_registration: false,
            software_name: String::new(),
            debug: false,
            event_publisher: None,
            delivery_max_attempts: DELIVERY_MAX_ATTEMPTS,
            delivery_initial_delay_secs: DELIVERY_INITIAL_DELAY_SECS,
            signed_fetch_actor_id: None,
            actor_cache_ttl_secs: ACTOR_CACHE_TTL_SECS,
            url_scheme: None,
            nodeinfo_services_inbound: vec![],
            nodeinfo_services_outbound: vec![],
            nodeinfo_metadata: serde_json::json!({}),
        }
    }

    pub fn activity_repo(mut self, activity_repo: Arc<dyn ActivityRepository>) -> Self {
        self.activity_repo = Some(activity_repo);
        self
    }
    pub fn follow_repo(mut self, follow_repo: Arc<dyn FollowRepository>) -> Self {
        self.follow_repo = Some(follow_repo);
        self
    }
    pub fn actor_repo(mut self, actor_repo: Arc<dyn ActorRepository>) -> Self {
        self.actor_repo = Some(actor_repo);
        self
    }
    pub fn blocklist_repo(mut self, blocklist_repo: Arc<dyn BlocklistRepository>) -> Self {
        self.blocklist_repo = Some(blocklist_repo);
        self
    }
    pub fn user_repo(mut self, user_repo: Arc<dyn ApUserRepository>) -> Self {
        self.user_repo = Some(user_repo);
        self
    }
    pub fn content_reader(mut self, content_reader: Arc<dyn ApContentReader>) -> Self {
        self.content_reader = Some(content_reader);
        self
    }
    pub fn object_handler(mut self, object_handler: Arc<dyn ApObjectHandler>) -> Self {
        self.object_handler = Some(object_handler);
        self
    }
    pub fn allow_registration(mut self, allow_registration: bool) -> Self {
        self.allow_registration = allow_registration;
        self
    }
    pub fn software_name(mut self, software_name: impl Into<String>) -> Self {
        self.software_name = software_name.into();
        self
    }
    pub fn debug(mut self, debug: bool) -> Self {
        self.debug = debug;
        self
    }
    pub fn event_publisher(
        mut self,
        event_publisher: Arc<dyn crate::data::EventPublisher>,
    ) -> Self {
        self.event_publisher = Some(event_publisher);
        self
    }
    pub fn delivery_max_attempts(mut self, delivery_max_attempts: u32) -> Self {
        self.delivery_max_attempts = delivery_max_attempts;
        self
    }
    pub fn delivery_initial_delay_secs(mut self, delivery_initial_delay_secs: u64) -> Self {
        self.delivery_initial_delay_secs = delivery_initial_delay_secs;
        self
    }

    pub fn actor_cache_ttl_secs(mut self, actor_cache_ttl_secs: u64) -> Self {
        self.actor_cache_ttl_secs = actor_cache_ttl_secs;
        self
    }

    pub fn nodeinfo_services(mut self, inbound: Vec<String>, outbound: Vec<String>) -> Self {
        self.nodeinfo_services_inbound = inbound;
        self.nodeinfo_services_outbound = outbound;
        self
    }

    pub fn nodeinfo_metadata(mut self, metadata: serde_json::Value) -> Self {
        self.nodeinfo_metadata = metadata;
        self
    }

    /// Override the default `/users/{uuid}` URL scheme. Consumers with custom
    /// actor paths should implement [`UrlScheme`] and pass it here.
    pub fn url_scheme(mut self, url_scheme: Arc<dyn UrlScheme>) -> Self {
        self.url_scheme = Some(url_scheme);
        self
    }

    /// Set a local actor whose keypair signs all outgoing fetch requests
    /// (HTTP Signature on GETs). Required for federating with instances
    /// that enforce authorized-fetch / Secure Mode.
    pub fn signed_fetch_actor_id(mut self, signed_fetch_actor_id: uuid::Uuid) -> Self {
        self.signed_fetch_actor_id = Some(signed_fetch_actor_id);
        self
    }

    pub async fn build(self) -> anyhow::Result<ActivityPubService> {
        let activity_repo = self
            .activity_repo
            .ok_or_else(|| anyhow::anyhow!("activity_repo required — call .activity_repo(arc)"))?;
        let follow_repo = self
            .follow_repo
            .ok_or_else(|| anyhow::anyhow!("follow_repo required — call .follow_repo(arc)"))?;
        let actor_repo = self
            .actor_repo
            .ok_or_else(|| anyhow::anyhow!("actor_repo required — call .actor_repo(arc)"))?;
        let blocklist_repo = self.blocklist_repo.ok_or_else(|| {
            anyhow::anyhow!("blocklist_repo required — call .blocklist_repo(arc)")
        })?;
        let user_repo = self
            .user_repo
            .ok_or_else(|| anyhow::anyhow!("user_repo required — call .user_repo(arc)"))?;
        let content_reader = self.content_reader.ok_or_else(|| {
            anyhow::anyhow!("content_reader required — call .content_reader(arc)")
        })?;
        let object_handler = self.object_handler.ok_or_else(|| {
            anyhow::anyhow!("object_handler required — call .object_handler(arc)")
        })?;
        let url_scheme = self
            .url_scheme
            .unwrap_or_else(|| Arc::new(DefaultUrlScheme));
        let data = FederationData::new(
            activity_repo,
            follow_repo,
            actor_repo.clone(),
            blocklist_repo,
            user_repo.clone(),
            content_reader,
            object_handler,
            self.base_url.clone(),
            self.allow_registration,
            self.software_name,
            self.event_publisher,
            std::time::Duration::from_secs(self.actor_cache_ttl_secs),
            url_scheme,
        )
        .with_nodeinfo_services(
            self.nodeinfo_services_inbound,
            self.nodeinfo_services_outbound,
        )
        .with_nodeinfo_metadata(self.nodeinfo_metadata);
        let signing_actor = if let Some(uid) = self.signed_fetch_actor_id {
            let actor = crate::actors::build_local_actor(
                uid,
                &self.base_url,
                user_repo.as_ref(),
                actor_repo.as_ref(),
                data.url_scheme.as_ref(),
            )
            .await?;
            Some(actor)
        } else {
            None
        };
        let federation_config =
            ApFederationConfig::new(data, self.debug, signing_actor.as_ref()).await?;
        Ok(ActivityPubService {
            federation_config,
            base_url: self.base_url,
            delivery_max_attempts: self.delivery_max_attempts,
            delivery_initial_delay_secs: self.delivery_initial_delay_secs,
        })
    }
}
