use async_trait::async_trait;
use url::Url;

#[derive(Debug, Clone)]
pub struct ApProfileField {
    pub name: String,
    pub value: String,
}

/// Resolved actor data returned by [`crate::service::ActivityPubService::lookup_actor_by_handle`].
/// Fetched via a signed HTTP request so strict instances (e.g. Threads) return full data.
#[derive(Debug, Clone)]
pub struct LookedUpActor {
    pub handle: String,
    pub display_name: Option<String>,
    pub bio: Option<String>,
    pub avatar_url: Option<Url>,
    pub banner_url: Option<Url>,
    pub ap_url: Url,
    pub outbox_url: Url,
    pub followers_url: Url,
    pub following_url: Url,
    pub also_known_as: Option<String>,
    pub profile_url: Option<Url>,
    pub attachment: Vec<ApProfileField>,
}

#[derive(Debug, Clone)]
pub struct ApUser {
    pub id: uuid::Uuid,
    pub username: String,
    pub bio: Option<String>,
    pub avatar_url: Option<Url>,
    pub banner_url: Option<Url>,
    pub also_known_as: Option<String>,
    pub profile_url: Option<Url>,
    pub attachment: Vec<ApProfileField>,
}

#[async_trait]
pub trait ApUserRepository: Send + Sync {
    async fn find_by_id(&self, id: uuid::Uuid) -> anyhow::Result<Option<ApUser>>;
    async fn find_by_username(&self, username: &str) -> anyhow::Result<Option<ApUser>>;
    async fn count_users(&self) -> anyhow::Result<usize>;
}
