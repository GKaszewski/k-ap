use anyhow::Result;
use async_trait::async_trait;

use super::types::BlockedDomain;

#[async_trait]
pub trait DomainBlocklist: Send + Sync {
    async fn add_blocked_domain(&self, domain: &str, reason: Option<&str>) -> Result<()>;
    async fn remove_blocked_domain(&self, domain: &str) -> Result<()>;
    async fn get_blocked_domains(&self) -> Result<Vec<BlockedDomain>>;
    async fn is_domain_blocked(&self, domain: &str) -> Result<bool>;
}
