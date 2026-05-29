use std::sync::Arc;

use activitypub_federation::{protocol::context::WithContext, traits::Object};
use axum::{Router, extract::DefaultBodyLimit, routing::get, routing::post};
use url::Url;

use crate::{
    actor_handler::actor_handler,
    actors::{DbActor, get_local_actor},
    content::{ApContentReader, ApObjectHandler},
    data::FederationData,
    featured_handler::featured_handler,
    federation::ApFederationConfig,
    followers_handler::{followers_handler, following_handler},
    inbox::inbox_handler,
    nodeinfo::{nodeinfo_handler, nodeinfo_well_known_handler},
    outbox::outbox_handler,
    repository::{
        ActivityRepository, ActorRepository, BlockedDomain, BlocklistRepository, FollowRepository,
    },
    user::ApUserRepository,
    webfinger::webfinger_handler,
};

mod backfill;
mod broadcast;
pub(super) mod delivery;
mod follow;

/// Default max delivery retries per inbox (used as the builder default).
pub const DELIVERY_MAX_ATTEMPTS: u32 = 3;
/// Default initial retry backoff in seconds; doubles each attempt.
pub const DELIVERY_INITIAL_DELAY_SECS: u64 = 1;
/// HTTP timeout when fetching remote AP resources.
pub const HTTP_FETCH_TIMEOUT_SECS: u64 = 30;
/// Sleep between backfill send batches.
pub const BATCH_FETCH_SLEEP_MS: u64 = 100;

#[derive(Clone)]
pub struct ActivityPubService {
    pub(super) federation_config: ApFederationConfig,
    pub(super) base_url: String,
    pub(super) delivery_max_attempts: u32,
    pub(super) delivery_initial_delay_secs: u64,
}

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
}

impl ActivityPubServiceBuilder {
    pub fn activity_repo(mut self, v: Arc<dyn ActivityRepository>) -> Self {
        self.activity_repo = Some(v);
        self
    }
    pub fn follow_repo(mut self, v: Arc<dyn FollowRepository>) -> Self {
        self.follow_repo = Some(v);
        self
    }
    pub fn actor_repo(mut self, v: Arc<dyn ActorRepository>) -> Self {
        self.actor_repo = Some(v);
        self
    }
    pub fn blocklist_repo(mut self, v: Arc<dyn BlocklistRepository>) -> Self {
        self.blocklist_repo = Some(v);
        self
    }
    pub fn user_repo(mut self, v: Arc<dyn ApUserRepository>) -> Self {
        self.user_repo = Some(v);
        self
    }
    pub fn content_reader(mut self, v: Arc<dyn ApContentReader>) -> Self {
        self.content_reader = Some(v);
        self
    }
    pub fn object_handler(mut self, v: Arc<dyn ApObjectHandler>) -> Self {
        self.object_handler = Some(v);
        self
    }
    pub fn allow_registration(mut self, v: bool) -> Self {
        self.allow_registration = v;
        self
    }
    pub fn software_name(mut self, v: impl Into<String>) -> Self {
        self.software_name = v.into();
        self
    }
    pub fn debug(mut self, v: bool) -> Self {
        self.debug = v;
        self
    }
    pub fn event_publisher(mut self, v: Arc<dyn crate::data::EventPublisher>) -> Self {
        self.event_publisher = Some(v);
        self
    }
    pub fn delivery_max_attempts(mut self, v: u32) -> Self {
        self.delivery_max_attempts = v;
        self
    }
    pub fn delivery_initial_delay_secs(mut self, v: u64) -> Self {
        self.delivery_initial_delay_secs = v;
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
        let data = FederationData::new(
            activity_repo,
            follow_repo,
            actor_repo,
            blocklist_repo,
            user_repo,
            content_reader,
            object_handler,
            self.base_url.clone(),
            self.allow_registration,
            self.software_name,
            self.event_publisher,
        );
        let federation_config = ApFederationConfig::new(data, self.debug).await?;
        Ok(ActivityPubService {
            federation_config,
            base_url: self.base_url,
            delivery_max_attempts: self.delivery_max_attempts,
            delivery_initial_delay_secs: self.delivery_initial_delay_secs,
        })
    }
}

impl ActivityPubService {
    pub fn builder(base_url: impl Into<String>) -> ActivityPubServiceBuilder {
        ActivityPubServiceBuilder {
            activity_repo: None,
            follow_repo: None,
            actor_repo: None,
            blocklist_repo: None,
            user_repo: None,
            content_reader: None,
            object_handler: None,
            base_url: base_url.into(),
            allow_registration: false,
            software_name: String::new(),
            debug: false,
            event_publisher: None,
            delivery_max_attempts: DELIVERY_MAX_ATTEMPTS,
            delivery_initial_delay_secs: DELIVERY_INITIAL_DELAY_SECS,
        }
    }

    pub fn federation_config(&self) -> &ApFederationConfig {
        &self.federation_config
    }
    pub fn request_data(&self) -> activitypub_federation::config::Data<FederationData> {
        self.federation_config.to_request_data()
    }
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// Returns the ActivityPub router. Inbox routes enforce a 1 MB body limit.
    pub fn router<S>(&self) -> Router<S>
    where
        S: Clone + Send + Sync + 'static,
    {
        Router::new()
            .route("/.well-known/nodeinfo", get(nodeinfo_well_known_handler))
            .route("/nodeinfo/2.0", get(nodeinfo_handler))
            .route("/.well-known/webfinger", get(webfinger_handler))
            .route(
                "/inbox",
                post(inbox_handler).layer(DefaultBodyLimit::max(1024 * 1024)),
            )
            .route("/users/{id}", get(actor_handler))
            .route(
                "/users/{id}/inbox",
                post(inbox_handler).layer(DefaultBodyLimit::max(1024 * 1024)),
            )
            .route("/users/{id}/outbox", get(outbox_handler))
            .route("/users/{id}/followers", get(followers_handler))
            .route("/users/{id}/following", get(following_handler))
            .route("/users/{id}/featured", get(featured_handler))
            .layer(self.federation_config.middleware())
    }

    pub async fn actor_json(&self, user_id_str: &str) -> anyhow::Result<String> {
        let uuid = uuid::Uuid::parse_str(user_id_str)?;
        let data = self.federation_config.to_request_data();
        let actor = get_local_actor(uuid, &data)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let person = actor
            .into_json(&data)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        Ok(serde_json::to_string(&WithContext::new(
            person,
            crate::urls::actor_ap_context(),
        ))?)
    }

    pub async fn followers_collection_json(
        &self,
        user_id: uuid::Uuid,
        page: Option<u32>,
    ) -> anyhow::Result<String> {
        const AP_CONTEXT: &str = "https://www.w3.org/ns/activitystreams";
        const PAGE_SIZE: usize = 20;
        let data = self.federation_config.to_request_data();
        let collection_id = format!("{}/users/{}/followers", self.base_url, user_id);
        let total = data.follow_repo.count_followers(user_id).await?;
        let obj = if let Some(p) = page {
            let p = p.max(1);
            let offset = (p.saturating_sub(1) as usize) * PAGE_SIZE;
            let followers = data
                .follow_repo
                .get_followers_page(user_id, offset as u32, PAGE_SIZE)
                .await?;
            let has_next = offset + followers.len() < total;
            let items: Vec<String> = followers.into_iter().map(|f| f.actor.url).collect();
            let mut obj = serde_json::json!({"@context":AP_CONTEXT,"type":"OrderedCollectionPage","id":format!("{}?page={}",collection_id,p),"partOf":collection_id,"totalItems":total,"orderedItems":items});
            if has_next {
                obj["next"] = serde_json::json!(format!("{}?page={}", collection_id, p + 1));
            }
            obj
        } else {
            serde_json::json!({"@context":AP_CONTEXT,"type":"OrderedCollection","id":collection_id,"totalItems":total,"first":format!("{}?page=1",collection_id)})
        };
        Ok(serde_json::to_string(&obj)?)
    }

    pub async fn following_collection_json(
        &self,
        user_id: uuid::Uuid,
        page: Option<u32>,
    ) -> anyhow::Result<String> {
        const AP_CONTEXT: &str = "https://www.w3.org/ns/activitystreams";
        const PAGE_SIZE: usize = 20;
        let data = self.federation_config.to_request_data();
        let collection_id = format!("{}/users/{}/following", self.base_url, user_id);
        let total = data.follow_repo.count_following(user_id).await?;
        let obj = if let Some(p) = page {
            let p = p.max(1);
            let offset = (p.saturating_sub(1) as usize) * PAGE_SIZE;
            let following = data
                .follow_repo
                .get_following_page(user_id, offset as u32, PAGE_SIZE)
                .await?;
            let has_next = offset + following.len() < total;
            let items: Vec<String> = following.into_iter().map(|a| a.url).collect();
            let mut obj = serde_json::json!({"@context":AP_CONTEXT,"type":"OrderedCollectionPage","id":format!("{}?page={}",collection_id,p),"partOf":collection_id,"totalItems":total,"orderedItems":items});
            if has_next {
                obj["next"] = serde_json::json!(format!("{}?page={}", collection_id, p + 1));
            }
            obj
        } else {
            serde_json::json!({"@context":AP_CONTEXT,"type":"OrderedCollection","id":collection_id,"totalItems":total,"first":format!("{}?page=1",collection_id)})
        };
        Ok(serde_json::to_string(&obj)?)
    }

    pub async fn mark_follower_accepted(
        &self,
        user_id: uuid::Uuid,
        actor_url: &str,
    ) -> anyhow::Result<()> {
        let data = self.federation_config.to_request_data();
        data.follow_repo
            .update_follower_status(
                user_id,
                actor_url,
                crate::repository::FollowerStatus::Accepted,
            )
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))
    }

    pub async fn mark_follower_rejected(
        &self,
        user_id: uuid::Uuid,
        actor_url: &str,
    ) -> anyhow::Result<()> {
        let data = self.federation_config.to_request_data();
        data.follow_repo
            .remove_follower(user_id, actor_url)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))
    }

    pub async fn lookup_actor_by_handle(
        &self,
        handle: &str,
    ) -> anyhow::Result<crate::user::LookedUpActor> {
        tracing::info!(handle, "looking up remote actor");
        let data = self.federation_config.to_request_data();
        let actor = self
            .webfinger_https(handle, &data)
            .await
            .inspect_err(|e| tracing::warn!(handle, error = %e, "actor lookup failed"))?;
        let domain = actor.ap_id.host_str().unwrap_or("").to_string();
        tracing::info!(handle = format!("{}@{}", actor.username, domain), ap_url = %actor.ap_id, "remote actor resolved");
        Ok(crate::user::LookedUpActor {
            handle: format!("{}@{}", actor.username, domain),
            display_name: actor.display_name,
            bio: actor.bio,
            avatar_url: actor.avatar_url,
            banner_url: actor.banner_url,
            ap_url: actor.ap_id,
            outbox_url: Some(actor.outbox_url),
            followers_url: Some(actor.followers_url),
            following_url: Some(actor.following_url),
            also_known_as: actor.also_known_as,
            profile_url: actor.profile_url,
            attachment: actor.attachment,
        })
    }

    pub async fn add_blocked_domain(
        &self,
        domain: &str,
        reason: Option<&str>,
    ) -> anyhow::Result<()> {
        let data = self.federation_config.to_request_data();
        data.blocklist_repo.add_blocked_domain(domain, reason).await
    }

    pub async fn remove_blocked_domain(&self, domain: &str) -> anyhow::Result<()> {
        let data = self.federation_config.to_request_data();
        data.blocklist_repo.remove_blocked_domain(domain).await
    }

    pub async fn get_blocked_domains(&self) -> anyhow::Result<Vec<BlockedDomain>> {
        let data = self.federation_config.to_request_data();
        data.blocklist_repo.get_blocked_domains().await
    }

    // ── Private helpers (accessible to child modules via Rust's privacy rules) ─

    async fn accepted_follower_inboxes(
        &self,
        data: &activitypub_federation::config::Data<FederationData>,
        local_user_id: uuid::Uuid,
    ) -> anyhow::Result<Option<(DbActor, Vec<Url>)>> {
        let local_actor = get_local_actor(local_user_id, data)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let inbox_strs = data
            .follow_repo
            .get_accepted_follower_inboxes(local_user_id)
            .await?;
        if inbox_strs.is_empty() {
            return Ok(None);
        }
        let inboxes: Vec<Url> = inbox_strs.into_iter().filter_map(|s| {
            Url::parse(&s).map_err(|e| tracing::warn!(inbox = %s, error = %e, "skipping unparseable inbox URL")).ok()
        }).collect();
        if inboxes.is_empty() {
            return Ok(None);
        }
        Ok(Some((local_actor, inboxes)))
    }

    async fn webfinger_https(
        &self,
        handle: &str,
        data: &activitypub_federation::config::Data<FederationData>,
    ) -> anyhow::Result<DbActor> {
        let normalized = handle.trim_start_matches('@');
        let at = normalized
            .rfind('@')
            .ok_or_else(|| anyhow::anyhow!("handle must be user@domain"))?;
        let (user, domain_str) = (&normalized[..at], &normalized[at + 1..]);
        let wf_url = format!(
            "https://{}/.well-known/webfinger?resource=acct:{}@{}",
            domain_str, user, domain_str
        );
        tracing::debug!(handle, wf_url, "resolving webfinger");
        let wf: serde_json::Value = reqwest::Client::new()
            .get(&wf_url)
            .header("Accept", "application/jrd+json, application/json")
            .send()
            .await?
            .json()
            .await?;
        let self_href = wf["links"]
            .as_array()
            .and_then(|links| {
                links.iter().find(|l| {
                    l["rel"].as_str() == Some("self")
                        && l["type"].as_str() == Some("application/activity+json")
                })
            })
            .and_then(|l| l["href"].as_str())
            .ok_or_else(|| anyhow::anyhow!("no self link in WebFinger response"))?
            .to_owned();
        tracing::debug!(handle, self_href, "webfinger resolved, fetching actor");
        let actor: DbActor =
            activitypub_federation::fetch::object_id::ObjectId::from(url::Url::parse(&self_href)?)
                .dereference(data)
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?;
        Ok(actor)
    }
}

#[cfg(test)]
mod tests {
    // Inbox deduplication and broadcast filtering are now tested via repository
    // integration tests in the consuming crate. See get_accepted_follower_inboxes.
}
