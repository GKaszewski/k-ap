pub mod activities;
pub mod actor_handler;
pub mod actors;
pub mod content;
pub mod data;
pub mod error;
pub mod featured_handler;
pub mod federation;
pub mod followers_handler;
pub mod inbox;
pub mod nodeinfo;
pub mod outbox;
pub mod repository;
pub mod service;
pub(crate) mod urls;
pub mod user;
pub mod webfinger;

pub use activitypub_federation::kinds::object::NoteType;
pub use content::{ApContentReader, ApObjectHandler};
pub use data::{EventPublisher, FederationData, FederationEvent};
pub use error::Error;
pub use federation::ApFederationConfig;
pub use repository::{
    ActivityRepository, ActorRepository, BlockedDomain, BlocklistRepository, FollowRepository,
    Follower, FollowerStatus, FollowingStatus, RemoteActor,
};
pub use service::ActivityPubService;
pub use urls::AS_PUBLIC;
pub use user::{
    ApActorType, ApProfileField, ApUser, ApUserRepository, ApVisibility, LookedUpActor,
};

#[cfg(test)]
#[path = "tests/integration.rs"]
mod integration_tests;
