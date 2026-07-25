use anyhow::Result;
use async_trait::async_trait;

use super::types::{FollowingStatus, RemoteActor};

/// Write operations for following relationships (outbound follows).
///
/// Used by:
/// - `service/follow.rs` (follow/unfollow/accept processing)
#[async_trait]
pub trait FollowingWriter: Send + Sync {
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
    async fn remove_following(&self, local_user_id: uuid::Uuid, actor_url: &str) -> Result<()>;
    async fn update_following_status(
        &self,
        local_user_id: uuid::Uuid,
        remote_actor_url: &str,
        status: FollowingStatus,
    ) -> Result<()>;
}
