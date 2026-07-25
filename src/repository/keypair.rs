use anyhow::Result;
use async_trait::async_trait;

use super::types::Keypair;

#[async_trait]
pub trait KeypairRepository: Send + Sync {
    async fn get_local_actor_keypair(&self, user_id: uuid::Uuid) -> Result<Option<Keypair>>;
    async fn save_local_actor_keypair(&self, user_id: uuid::Uuid, keypair: Keypair) -> Result<()>;
}
