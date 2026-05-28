use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use url::Url;

#[derive(Debug, Clone)]
pub struct ApProfileField {
    pub name: String,
    pub value: String,
}

/// Actor type for AP serialization. Defaults to `Person`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ApActorType {
    Person,
    Service,
    Application,
    Organization,
    Group,
}

impl Default for ApActorType {
    fn default() -> Self {
        Self::Person
    }
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
    pub outbox_url: Option<Url>,
    pub followers_url: Option<Url>,
    pub following_url: Option<Url>,
    pub also_known_as: Option<String>,
    pub profile_url: Option<Url>,
    pub attachment: Vec<ApProfileField>,
}

#[derive(Debug, Clone)]
pub struct ApUser {
    pub id: uuid::Uuid,
    pub username: String,
    pub display_name: Option<String>,
    pub bio: Option<String>,
    pub avatar_url: Option<Url>,
    pub banner_url: Option<Url>,
    pub also_known_as: Option<String>,
    pub profile_url: Option<Url>,
    pub attachment: Vec<ApProfileField>,
    /// If true, incoming Follow requests must be manually approved before the
    /// actor is listed as `manuallyApprovesFollowers=true` in AP JSON.
    pub manually_approves_followers: bool,
    /// AP actor type serialized in the actor JSON. Defaults to `Person`.
    pub actor_type: ApActorType,
}

#[async_trait]
pub trait ApUserRepository: Send + Sync {
    async fn find_by_id(&self, id: uuid::Uuid) -> anyhow::Result<Option<ApUser>>;
    async fn find_by_username(&self, username: &str) -> anyhow::Result<Option<ApUser>>;
    async fn count_users(&self) -> anyhow::Result<usize>;
}
