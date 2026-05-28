use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use url::Url;

#[derive(Debug, Clone)]
pub struct ApProfileField {
    pub name: String,
    pub value: String,
}

/// Visibility of a federated post.
///
/// Controls the `to`/`cc` addressing fields of outbound Create/Update activities
/// and whether the library fans the activity out to followers at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ApVisibility {
    /// `to: [AS_PUBLIC], cc: [followers]` — fully public, indexable by search engines.
    #[default]
    Public,
    /// `to: [followers], cc: []` — only followers receive it, not indexed publicly.
    FollowersOnly,
    /// No federation delivery. The library returns immediately without sending anything.
    /// Use when the post should exist only on the local instance.
    Private,
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
    /// Whether this actor should appear in AP directory listings.
    /// Serialized as `discoverable` in actor JSON. Defaults to `true`.
    pub discoverable: bool,
    /// AP actor type serialized in the actor JSON. Defaults to `Person`.
    pub actor_type: ApActorType,
    /// URL of the `featured` (pinned posts) collection. Set to expose a pinned
    /// posts collection in the actor JSON, compatible with Mastodon/Pleroma.
    pub featured_url: Option<Url>,
}

#[async_trait]
pub trait ApUserRepository: Send + Sync {
    async fn find_by_id(&self, id: uuid::Uuid) -> anyhow::Result<Option<ApUser>>;
    async fn find_by_username(&self, username: &str) -> anyhow::Result<Option<ApUser>>;
    async fn count_users(&self) -> anyhow::Result<usize>;
}
