use super::{AnnounceRepository, KeypairRepository, RemoteActorCache};

/// Manages local actor keypairs, remote actor cache, and Announce tracking.
pub trait ActorRepository: KeypairRepository + RemoteActorCache + AnnounceRepository {}
impl<T: KeypairRepository + RemoteActorCache + AnnounceRepository> ActorRepository for T {}
