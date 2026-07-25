use anyhow::Result;
use async_trait::async_trait;

use super::types::RemoteActor;

#[async_trait]
pub trait RemoteActorCache: Send + Sync {
    async fn upsert_remote_actor(&self, actor: RemoteActor) -> Result<()>;
    async fn get_remote_actor(&self, actor_url: &str) -> Result<Option<RemoteActor>>;
}
