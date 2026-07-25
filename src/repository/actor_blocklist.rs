use anyhow::Result;
use async_trait::async_trait;

#[async_trait]
pub trait ActorBlocklist: Send + Sync {
    async fn add_blocked_actor(&self, local_user_id: uuid::Uuid, actor_url: &str) -> Result<()>;
    async fn remove_blocked_actor(&self, local_user_id: uuid::Uuid, actor_url: &str) -> Result<()>;
    async fn get_blocked_actors(&self, local_user_id: uuid::Uuid) -> Result<Vec<String>>;
    async fn is_actor_blocked(&self, local_user_id: uuid::Uuid, actor_url: &str) -> Result<bool>;
}
