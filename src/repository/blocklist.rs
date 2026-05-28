use anyhow::Result;
use async_trait::async_trait;

use super::BlockedDomain;

/// Domain and actor-level blocklists.
#[async_trait]
pub trait BlocklistRepository: Send + Sync {
    // ── Domain blocklist ────────────────────────────────────────────────────
    async fn add_blocked_domain(&self, domain: &str, reason: Option<&str>) -> Result<()>;
    async fn remove_blocked_domain(&self, domain: &str) -> Result<()>;
    async fn get_blocked_domains(&self) -> Result<Vec<BlockedDomain>>;
    async fn is_domain_blocked(&self, domain: &str) -> Result<bool>;

    // ── Per-user actor blocklist ────────────────────────────────────────────
    async fn add_blocked_actor(
        &self,
        local_user_id: uuid::Uuid,
        actor_url: &str,
    ) -> Result<()>;
    async fn remove_blocked_actor(
        &self,
        local_user_id: uuid::Uuid,
        actor_url: &str,
    ) -> Result<()>;
    async fn get_blocked_actors(
        &self,
        local_user_id: uuid::Uuid,
    ) -> Result<Vec<String>>;
    async fn is_actor_blocked(
        &self,
        local_user_id: uuid::Uuid,
        actor_url: &str,
    ) -> Result<bool>;
}
