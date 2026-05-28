use async_trait::async_trait;
use chrono::{DateTime, Utc};
use url::Url;

/// Read side — the library queries this when sending content outward.
/// Implement on the same struct as [`ApObjectHandler`] if you prefer
/// a single database type.
#[async_trait]
pub trait ApContentReader: Send + Sync {
    /// All locally-authored objects for this user. Used by backfill on accept_follower.
    async fn get_local_objects_for_user(
        &self,
        user_id: uuid::Uuid,
    ) -> anyhow::Result<Vec<(Url, serde_json::Value)>>;

    /// Newest-first page of locally-authored objects, published before `before`.
    /// Returns `(ap_id, object_json, published_at)`. Used by the outbox endpoint.
    async fn get_local_objects_page(
        &self,
        user_id: uuid::Uuid,
        before: Option<DateTime<Utc>>,
        limit: usize,
    ) -> anyhow::Result<Vec<(Url, serde_json::Value, DateTime<Utc>)>>;

    /// Total locally-authored posts across all users. Used by NodeInfo.
    async fn count_local_posts(&self) -> anyhow::Result<u64>;
}

/// Write side — the library calls these when processing inbound AP activities.
#[async_trait]
pub trait ApObjectHandler: Send + Sync {
    async fn on_create(
        &self,
        ap_id: &Url,
        actor_url: &Url,
        object: serde_json::Value,
    ) -> anyhow::Result<()>;

    async fn on_update(
        &self,
        ap_id: &Url,
        actor_url: &Url,
        object: serde_json::Value,
    ) -> anyhow::Result<()>;

    async fn on_delete(&self, ap_id: &Url, actor_url: &Url) -> anyhow::Result<()>;

    async fn on_actor_removed(&self, actor_url: &Url) -> anyhow::Result<()>;

    async fn on_like(&self, object_url: &Url, actor_url: &Url) -> anyhow::Result<()>;

    async fn on_unlike(&self, object_url: &Url, actor_url: &Url) -> anyhow::Result<()>;

    async fn on_announce_received(
        &self,
        object_url: &Url,
        actor_url: &Url,
    ) -> anyhow::Result<()>;

    async fn on_announce_of_remote(
        &self,
        object_url: &Url,
        actor_url: &Url,
    ) -> anyhow::Result<()>;

    async fn on_mention(
        &self,
        thought_ap_id: &Url,
        mentioned_user_uuid: uuid::Uuid,
        actor_url: &Url,
    ) -> anyhow::Result<()>;
}
