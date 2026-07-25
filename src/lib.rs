pub(crate) mod activities;
pub(crate) mod actors;
pub(crate) mod content;
pub(crate) mod data;
pub(crate) mod error;
pub(crate) mod federation;
pub(crate) mod handlers;
pub mod repository;
pub(crate) mod security;
pub mod service;
/// Mock builders for testing. Not behind `#[cfg(test)]` so downstream crates
/// can use them in their own test suites.
pub mod testing;
pub(crate) mod url_scheme;
pub(crate) mod urls;
pub(crate) mod user;

pub use content::{ApContentReader, ApObjectHandler, LocalObject};
pub use data::{EventPublisher, FederationData, FederationEvent};
pub use error::Error;
pub use federation::ApFederationConfig;
pub use handlers::actor::actor_handler;
pub use handlers::followers::{followers_handler, following_handler};
pub use repository::{
    ActivityRepository, ActorBlocklist, ActorRepository, AnnounceRepository, BlockedDomain,
    BlocklistRepository, DomainBlocklist, FollowMigration, FollowRepository, Follower,
    FollowerReader, FollowerStatus, FollowerWriter, FollowingReader, FollowingStatus,
    FollowingWriter, Keypair, KeypairRepository, RemoteActor, RemoteActorCache,
};
pub use service::ActivityPubService;
pub use url_scheme::{DefaultUrlScheme, UrlScheme};
pub use urls::{AP_CONTENT_TYPE, AP_CONTEXT, AS_PUBLIC, INBOX_BODY_LIMIT};
pub use user::{
    ApActorType, ApProfileField, ApUser, ApUserRepository, ApVisibility, LookedUpActor,
};

#[cfg(test)]
#[path = "tests/integration.rs"]
mod integration_tests;

#[cfg(test)]
#[path = "tests/activities.rs"]
mod activity_tests;

#[cfg(test)]
#[path = "tests/broadcast.rs"]
mod broadcast_tests;
