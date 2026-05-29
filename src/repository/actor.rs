use anyhow::Result;
use async_trait::async_trait;

use super::RemoteActor;

/// Manages local actor keypairs, remote actor cache, and Announce tracking.
#[async_trait]
pub trait ActorRepository: Send + Sync {
    // ── Local keypairs ──────────────────────────────────────────────────────
    async fn get_local_actor_keypair(
        &self,
        user_id: uuid::Uuid,
    ) -> Result<Option<(String, String)>>;
    async fn save_local_actor_keypair(
        &self,
        user_id: uuid::Uuid,
        public_key: String,
        private_key: String,
    ) -> Result<()>;

    // ── Remote actor cache ──────────────────────────────────────────────────
    async fn upsert_remote_actor(&self, actor: RemoteActor) -> Result<()>;
    async fn get_remote_actor(&self, actor_url: &str) -> Result<Option<RemoteActor>>;

    // ── Boost (Announce) tracking ───────────────────────────────────────────
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
