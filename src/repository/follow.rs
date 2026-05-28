use anyhow::Result;
use async_trait::async_trait;

use super::{Follower, FollowerStatus, FollowingStatus, RemoteActor};

/// Manages follower/following relationships and account migration.
#[async_trait]
pub trait FollowRepository: Send + Sync {
    // ── Inbound followers ───────────────────────────────────────────────────
    async fn add_follower(
        &self,
        local_user_id: uuid::Uuid,
        remote_actor_url: &str,
        status: FollowerStatus,
        follow_activity_id: &str,
    ) -> Result<()>;
    async fn get_follower_follow_activity_id(
        &self,
        local_user_id: uuid::Uuid,
        remote_actor_url: &str,
    ) -> Result<Option<String>>;
    async fn remove_follower(
        &self,
        local_user_id: uuid::Uuid,
        remote_actor_url: &str,
    ) -> Result<()>;
    async fn get_followers(&self, local_user_id: uuid::Uuid) -> Result<Vec<Follower>>;
    async fn get_followers_page(
        &self,
        local_user_id: uuid::Uuid,
        offset: u32,
        limit: usize,
    ) -> Result<Vec<Follower>>;
    async fn count_followers(&self, local_user_id: uuid::Uuid) -> Result<usize>;
    async fn update_follower_status(
        &self,
        local_user_id: uuid::Uuid,
        remote_actor_url: &str,
        status: FollowerStatus,
    ) -> Result<()>;
    async fn get_pending_followers(
        &self,
        local_user_id: uuid::Uuid,
    ) -> Result<Vec<RemoteActor>>;
    /// Return deduplicated inbox URLs (shared_inbox preferred) for accepted
    /// followers, excluding blocked actors/domains. DB-side filtering.
    async fn get_accepted_follower_inboxes(
        &self,
        local_user_id: uuid::Uuid,
    ) -> Result<Vec<String>>;

    // ── Outbound following ──────────────────────────────────────────────────
    async fn add_following(
        &self,
        local_user_id: uuid::Uuid,
        actor: RemoteActor,
        follow_activity_id: &str,
    ) -> Result<()>;
    async fn get_follow_activity_id(
        &self,
        local_user_id: uuid::Uuid,
        remote_actor_url: &str,
    ) -> Result<Option<String>>;
    async fn remove_following(
        &self,
        local_user_id: uuid::Uuid,
        actor_url: &str,
    ) -> Result<()>;
    async fn get_following(&self, local_user_id: uuid::Uuid) -> Result<Vec<RemoteActor>>;
    async fn get_following_page(
        &self,
        local_user_id: uuid::Uuid,
        offset: u32,
        limit: usize,
    ) -> Result<Vec<RemoteActor>>;
    async fn count_following(&self, local_user_id: uuid::Uuid) -> Result<usize>;
    async fn update_following_status(
        &self,
        local_user_id: uuid::Uuid,
        remote_actor_url: &str,
        status: FollowingStatus,
    ) -> Result<()>;
    async fn get_following_outbox_url(
        &self,
        local_user_id: uuid::Uuid,
        remote_actor_url: &str,
    ) -> Result<Option<String>>;

    // ── Account migration ───────────────────────────────────────────────────
    /// Migrate all follower records from `old_actor_url` to `new_actor_url`.
    /// Returns local user IDs that need a re-follow sent.
    async fn migrate_follower_actor(
        &self,
        old_actor_url: &str,
        new_actor_url: &str,
    ) -> Result<Vec<uuid::Uuid>>;
}
