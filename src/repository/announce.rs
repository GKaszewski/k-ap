use anyhow::Result;
use async_trait::async_trait;

#[async_trait]
pub trait AnnounceRepository: Send + Sync {
    async fn add_announce(
        &self,
        activity_id: &str,
        object_url: &str,
        actor_url: &str,
        announced_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<()>;
    /// Remove a boost record when a remote actor sends `Undo(Announce)`.
    /// Implementations should match by `activity_id` and `actor_url`.
    async fn remove_announce(&self, activity_id: &str, actor_url: &str) -> Result<()>;
    async fn count_announces(&self, object_url: &str) -> Result<usize>;
}
