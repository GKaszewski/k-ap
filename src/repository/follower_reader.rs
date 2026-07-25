use anyhow::Result;
use async_trait::async_trait;

use super::types::{Follower, RemoteActor};

/// Read-only view of follower relationships.
///
/// Used by:
/// - `ActivityPubService::accepted_follower_inboxes` (via `get_accepted_follower_inboxes`)
/// - `service/collections.rs` (via `count_followers`, `get_followers_page`)
/// - `handlers/followers.rs` (via `count_followers`, `get_followers_page`)
/// - `service/broadcast.rs` (via `get_accepted_follower_inboxes`)
#[async_trait]
pub trait FollowerReader: Send + Sync {
    async fn get_followers(&self, local_user_id: uuid::Uuid) -> Result<Vec<Follower>>;
    async fn get_followers_page(
        &self,
        local_user_id: uuid::Uuid,
        offset: u32,
        limit: usize,
    ) -> Result<Vec<Follower>>;
    async fn count_followers(&self, local_user_id: uuid::Uuid) -> Result<usize>;
    async fn get_pending_followers(&self, local_user_id: uuid::Uuid) -> Result<Vec<RemoteActor>>;
    /// Return deduplicated inbox URLs (shared_inbox preferred) for accepted
    /// followers, excluding blocked actors/domains. DB-side filtering.
    async fn get_accepted_follower_inboxes(&self, local_user_id: uuid::Uuid)
    -> Result<Vec<String>>;
    /// Count of accepted followers only. More efficient than loading all followers
    /// and filtering in application memory.
    async fn count_accepted_followers(&self, local_user_id: uuid::Uuid) -> Result<usize>;
    /// Accepted followers page for display purposes. `offset` is 0-based.
    async fn get_accepted_followers_page(
        &self,
        local_user_id: uuid::Uuid,
        offset: u32,
        limit: usize,
    ) -> Result<Vec<RemoteActor>>;
}
