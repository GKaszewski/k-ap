use anyhow::Result;
use async_trait::async_trait;

use super::types::FollowerStatus;

/// Write operations for follower relationships.
///
/// Used by:
/// - inbox handlers (Accept/Follow/Undo processing)
/// - `service/lookup.rs` (via `update_follower_status`, `remove_follower`)
#[async_trait]
pub trait FollowerWriter: Send + Sync {
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
    async fn update_follower_status(
        &self,
        local_user_id: uuid::Uuid,
        remote_actor_url: &str,
        status: FollowerStatus,
    ) -> Result<()>;
}
