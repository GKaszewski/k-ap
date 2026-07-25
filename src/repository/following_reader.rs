use anyhow::Result;
use async_trait::async_trait;

use super::types::RemoteActor;

/// Read-only view of following relationships (accounts this user follows).
///
/// Used by:
/// - `service/collections.rs` (via `count_following`, `get_following_page`)
/// - `handlers/followers.rs` (via `count_following`, `get_following_page`)
#[async_trait]
pub trait FollowingReader: Send + Sync {
    async fn get_following(&self, local_user_id: uuid::Uuid) -> Result<Vec<RemoteActor>>;
    async fn get_following_page(
        &self,
        local_user_id: uuid::Uuid,
        offset: u32,
        limit: usize,
    ) -> Result<Vec<RemoteActor>>;
    async fn count_following(&self, local_user_id: uuid::Uuid) -> Result<usize>;
}
