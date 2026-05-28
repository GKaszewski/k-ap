use anyhow::Result;
use async_trait::async_trait;

/// Tracks which inbound AP activity IDs have already been processed.
/// Prevents duplicate handling when remote servers retry delivery.
/// Implementations should enforce a UNIQUE constraint on stored IDs.
#[async_trait]
pub trait ActivityRepository: Send + Sync {
    async fn is_activity_processed(&self, activity_id: &str) -> Result<bool>;
    async fn mark_activity_processed(&self, activity_id: &str) -> Result<()>;
}
