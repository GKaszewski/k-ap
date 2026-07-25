use anyhow::Result;
use async_trait::async_trait;

/// Handles account migration by remapping follower records from one actor URL
/// to another.
///
/// Used by:
/// - `activities/move_act.rs` (Move activity processing)
///
/// Most implementations can use the provided default no-op if account
/// migration is not supported.
#[async_trait]
pub trait FollowMigration: Send + Sync {
    /// Migrate all follower records from `old_actor_url` to `new_actor_url`.
    /// Returns local user IDs that need a re-follow sent.
    ///
    /// The default implementation is a no-op returning an empty list, suitable
    /// for deployments that do not support account migration.
    async fn migrate_follower_actor(
        &self,
        _old_actor_url: &str,
        _new_actor_url: &str,
    ) -> Result<Vec<uuid::Uuid>> {
        Ok(vec![])
    }
}
