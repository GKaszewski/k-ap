use url::Url;

use axum::{Router, extract::DefaultBodyLimit, routing::get, routing::post};

use crate::{
    actors::{DbActor, get_local_actor},
    data::FederationData,
    federation::ApFederationConfig,
    handlers::{
        featured::featured_handler,
        inbox::inbox_handler,
        nodeinfo::{nodeinfo_handler, nodeinfo_well_known_handler},
        outbox::outbox_handler,
        webfinger::webfinger_handler,
    },
};

mod backfill;
pub(crate) mod broadcast;
mod builder;
pub(crate) mod collections;
pub(super) mod delivery;
mod fetch;
mod follow;
mod lookup;
pub(crate) mod types;

pub use builder::ActivityPubServiceBuilder;

/// Default max delivery retries per inbox (used as the builder default).
pub const DELIVERY_MAX_ATTEMPTS: u32 = 3;
/// Default initial retry backoff in seconds; doubles each attempt.
pub const DELIVERY_INITIAL_DELAY_SECS: u64 = 1;
/// HTTP timeout when fetching remote AP resources.
pub const HTTP_FETCH_TIMEOUT_SECS: u64 = 30;
/// Sleep between backfill send batches.
pub const BATCH_FETCH_SLEEP_MS: u64 = 100;
/// Default actor cache TTL in seconds (24 hours).
pub const ACTOR_CACHE_TTL_SECS: u64 = 24 * 60 * 60;

#[derive(Clone)]
pub struct ActivityPubService {
    pub(super) federation_config: ApFederationConfig,
    pub(super) base_url: String,
    pub(super) delivery_max_attempts: u32,
    pub(super) delivery_initial_delay_secs: u64,
}

impl ActivityPubService {
    pub fn builder(base_url: impl Into<String>) -> ActivityPubServiceBuilder {
        ActivityPubServiceBuilder::new(base_url.into())
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

    /// Returns the ActivityPub router.
    ///
    /// Registers only routes that k-ap fully owns:
    /// - `POST /inbox` + `POST /users/{id}/inbox` — signature verification + dispatch (1 MB limit)
    /// - `GET /users/{id}/outbox` — cursor-paginated OrderedCollection
    /// - `GET /users/{id}/featured` — pinned posts OrderedCollection
    /// - `GET /.well-known/webfinger`, `GET /.well-known/nodeinfo`, `GET /nodeinfo/2.0`
    ///
    /// **Not registered:** `GET /users/{id}`, `GET /users/{id}/followers`,
    /// `GET /users/{id}/following`. Real applications need those paths to serve
    /// both AP JSON and their own UI JSON (content negotiation), so they must own
    /// the route. Call `actor_json`, `followers_collection_json`, and
    /// `following_collection_json` from your own handler to produce the AP response.
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
                post(inbox_handler).layer(DefaultBodyLimit::max(crate::urls::INBOX_BODY_LIMIT)),
            )
            .route(
                "/users/{id}/inbox",
                post(inbox_handler).layer(DefaultBodyLimit::max(crate::urls::INBOX_BODY_LIMIT)),
            )
            .route("/users/{id}/outbox", get(outbox_handler))
            .route("/users/{id}/featured", get(featured_handler))
            .layer(self.federation_config.middleware())
    }

    async fn accepted_follower_inboxes(
        &self,
        data: &activitypub_federation::config::Data<FederationData>,
        local_user_id: uuid::Uuid,
    ) -> anyhow::Result<Option<(DbActor, Vec<Url>)>> {
        let local_actor = get_local_actor(local_user_id, data).await?;
        let inbox_strs = data
            .follow_repo
            .get_accepted_follower_inboxes(local_user_id)
            .await?;
        if inbox_strs.is_empty() {
            return Ok(None);
        }
        let inboxes: Vec<Url> = inbox_strs.into_iter().filter_map(|inbox_str| {
            Url::parse(&inbox_str).map_err(|e| tracing::warn!(inbox = %inbox_str, error = %e, "skipping unparseable inbox URL")).ok()
        }).collect();
        if inboxes.is_empty() {
            return Ok(None);
        }
        Ok(Some((local_actor, inboxes)))
    }
}
