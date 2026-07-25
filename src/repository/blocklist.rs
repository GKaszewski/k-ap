use super::{ActorBlocklist, DomainBlocklist};

/// Domain and actor-level blocklists.
pub trait BlocklistRepository: DomainBlocklist + ActorBlocklist {}
impl<T: DomainBlocklist + ActorBlocklist> BlocklistRepository for T {}
