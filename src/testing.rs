//! Mock builders for testing all k-ap traits.
//!
//! **Not behind `#[cfg(test)]`** so downstream consumers (e.g. movies-diary)
//! can use these mocks in their own test suites.
//!
//! # Usage
//!
//! ```ignore
//! let follow_repo = MockFollowRepoBuilder::new()
//!     .on_add_follower(|id, url, status, _| {
//!         // custom assertion / tracking
//!         Ok(())
//!     })
//!     .build();
//! ```

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use url::Url;

use crate::content::{ApContentReader, ApObjectHandler, LocalObject};
use crate::data::EventPublisher;
use crate::data::FederationEvent;
use crate::repository::{
    ActivityRepository, ActorBlocklist, AnnounceRepository, BlockedDomain, DomainBlocklist,
    FollowMigration, Follower, FollowerReader, FollowerStatus, FollowerWriter, FollowingReader,
    FollowingStatus, FollowingWriter, Keypair, KeypairRepository, RemoteActor, RemoteActorCache,
};
use crate::user::{ApUser, ApUserRepository};

/// Generate a mock struct + builder + trait impls from a compact spec.
///
/// Each method stores a `Box<dyn Fn(…) -> anyhow::Result<Ret>>` closure.
/// The builder defaults every unset method to `Ok(Default::default())`.
macro_rules! mock_repo {
    (
        $mock:ident, $builder:ident {
            $(
                trait $trait_name:ident {
                    $(
                        fn $method:ident( $( $pname:ident : $pty:ty ),* $(,)? ) -> $ret:ty;
                    )*
                }
            )*
        }
    ) => {
        pub struct $mock {
            $($(
                $method: Box<dyn Fn($($pty),*) -> anyhow::Result<$ret> + Send + Sync>,
            )*)*
        }

        pub struct $builder {
            $($(
                $method: Option<Box<dyn Fn($($pty),*) -> anyhow::Result<$ret> + Send + Sync>>,
            )*)*
        }

        impl $builder {
            pub fn new() -> Self {
                Self {
                    $($(
                        $method: None,
                    )*)*
                }
            }

            paste::paste! {
                $($(
                    pub fn [<on_ $method>](
                        mut self,
                        f: impl Fn($($pty),*) -> anyhow::Result<$ret> + Send + Sync + 'static,
                    ) -> Self {
                        self.$method = Some(Box::new(f));
                        self
                    }
                )*)*
            }

            pub fn build(self) -> Arc<$mock> {
                Arc::new($mock {
                    $($(
                        $method: self.$method.unwrap_or_else(||
                            Box::new(|$(_: $pty),*| Ok(Default::default()))
                        ),
                    )*)*
                })
            }
        }

        impl Default for $builder {
            fn default() -> Self {
                Self::new()
            }
        }

        $(
            #[async_trait]
            impl $trait_name for $mock {
                $(
                    async fn $method(&self, $($pname: $pty),*) -> anyhow::Result<$ret> {
                        (self.$method)($($pname),*)
                    }
                )*
            }
        )*
    };
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// MockFollowRepo
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

mock_repo! {
    MockFollowRepo, MockFollowRepoBuilder {
        trait FollowerWriter {
            fn add_follower(
                local_user_id: uuid::Uuid,
                remote_actor_url: &str,
                status: FollowerStatus,
                follow_activity_id: &str
            ) -> ();
            fn get_follower_follow_activity_id(
                local_user_id: uuid::Uuid,
                remote_actor_url: &str
            ) -> Option<String>;
            fn remove_follower(
                local_user_id: uuid::Uuid,
                remote_actor_url: &str
            ) -> ();
            fn update_follower_status(
                local_user_id: uuid::Uuid,
                remote_actor_url: &str,
                status: FollowerStatus
            ) -> ();
        }
        trait FollowerReader {
            fn get_followers(local_user_id: uuid::Uuid) -> Vec<Follower>;
            fn get_followers_page(
                local_user_id: uuid::Uuid,
                offset: u32,
                limit: usize
            ) -> Vec<Follower>;
            fn count_followers(local_user_id: uuid::Uuid) -> usize;
            fn get_pending_followers(local_user_id: uuid::Uuid) -> Vec<RemoteActor>;
            fn get_accepted_follower_inboxes(local_user_id: uuid::Uuid) -> Vec<String>;
            fn count_accepted_followers(local_user_id: uuid::Uuid) -> usize;
            fn get_accepted_followers_page(
                local_user_id: uuid::Uuid,
                offset: u32,
                limit: usize
            ) -> Vec<RemoteActor>;
        }
        trait FollowingWriter {
            fn add_following(
                local_user_id: uuid::Uuid,
                actor: RemoteActor,
                follow_activity_id: &str
            ) -> ();
            fn get_follow_activity_id(
                local_user_id: uuid::Uuid,
                remote_actor_url: &str
            ) -> Option<String>;
            fn remove_following(
                local_user_id: uuid::Uuid,
                actor_url: &str
            ) -> ();
            fn update_following_status(
                local_user_id: uuid::Uuid,
                remote_actor_url: &str,
                status: FollowingStatus
            ) -> ();
        }
        trait FollowingReader {
            fn get_following(local_user_id: uuid::Uuid) -> Vec<RemoteActor>;
            fn get_following_page(
                local_user_id: uuid::Uuid,
                offset: u32,
                limit: usize
            ) -> Vec<RemoteActor>;
            fn count_following(local_user_id: uuid::Uuid) -> usize;
        }
        trait FollowMigration {
            fn migrate_follower_actor(
                old_actor_url: &str,
                new_actor_url: &str
            ) -> Vec<uuid::Uuid>;
        }
    }
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// MockActorRepo
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

mock_repo! {
    MockActorRepo, MockActorRepoBuilder {
        trait KeypairRepository {
            fn get_local_actor_keypair(user_id: uuid::Uuid) -> Option<Keypair>;
            fn save_local_actor_keypair(user_id: uuid::Uuid, keypair: Keypair) -> ();
        }
        trait RemoteActorCache {
            fn upsert_remote_actor(actor: RemoteActor) -> ();
            fn get_remote_actor(actor_url: &str) -> Option<RemoteActor>;
        }
        trait AnnounceRepository {
            fn add_announce(
                activity_id: &str,
                object_url: &str,
                actor_url: &str,
                announced_at: DateTime<Utc>
            ) -> ();
            fn remove_announce(activity_id: &str, actor_url: &str) -> ();
            fn count_announces(object_url: &str) -> usize;
        }
    }
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// MockBlocklistRepo
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

mock_repo! {
    MockBlocklistRepo, MockBlocklistRepoBuilder {
        trait DomainBlocklist {
            fn add_blocked_domain(domain: &str, reason: Option<&str>) -> ();
            fn remove_blocked_domain(domain: &str) -> ();
            fn get_blocked_domains() -> Vec<BlockedDomain>;
            fn is_domain_blocked(domain: &str) -> bool;
        }
        trait ActorBlocklist {
            fn add_blocked_actor(local_user_id: uuid::Uuid, actor_url: &str) -> ();
            fn remove_blocked_actor(local_user_id: uuid::Uuid, actor_url: &str) -> ();
            fn get_blocked_actors(local_user_id: uuid::Uuid) -> Vec<String>;
            fn is_actor_blocked(local_user_id: uuid::Uuid, actor_url: &str) -> bool;
        }
    }
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// MockActivityRepo
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

mock_repo! {
    MockActivityRepo, MockActivityRepoBuilder {
        trait ActivityRepository {
            fn is_activity_processed(activity_id: &str) -> bool;
            fn mark_activity_processed(activity_id: &str) -> ();
        }
    }
}

mock_repo! {
    MockUserRepo, MockUserRepoBuilder {
        trait ApUserRepository {
            fn find_by_id(id: uuid::Uuid) -> Option<ApUser>;
            fn find_by_username(username: &str) -> Option<ApUser>;
            fn count_users() -> usize;
        }
    }
}

mock_repo! {
    MockContentReader, MockContentReaderBuilder {
        trait ApContentReader {
            fn get_local_objects_page(
                user_id: uuid::Uuid,
                before: Option<DateTime<Utc>>,
                limit: usize
            ) -> Vec<LocalObject>;
            fn count_local_posts() -> u64;
            fn get_featured_objects(user_id: uuid::Uuid) -> Vec<Url>;
        }
    }
}

mock_repo! {
    MockObjectHandler, MockObjectHandlerBuilder {
        trait ApObjectHandler {
            fn on_create(ap_id: &Url, actor_url: &Url, object: serde_json::Value) -> ();
            fn on_update(ap_id: &Url, actor_url: &Url, object: serde_json::Value) -> ();
            fn on_delete(ap_id: &Url, actor_url: &Url) -> ();
            fn on_actor_removed(actor_url: &Url) -> ();
            fn on_like(object_url: &Url, actor_url: &Url) -> ();
            fn on_unlike(object_url: &Url, actor_url: &Url) -> ();
            fn on_announce_received(object_url: &Url, actor_url: &Url) -> ();
            fn on_announce_removed(object_url: &Url, actor_url: &Url) -> ();
            fn on_announce_of_remote(object_url: &Url, actor_url: &Url) -> ();
            fn on_mention(
                thought_ap_id: &Url,
                mentioned_user_uuid: uuid::Uuid,
                actor_url: &Url
            ) -> ();
            fn on_unknown_activity(
                activity_type: &str,
                activity: serde_json::Value,
                actor_url: &Url
            ) -> ();
        }
    }
}

mock_repo! {
    MockEventPublisher, MockEventPublisherBuilder {
        trait EventPublisher {
            fn publish(event: FederationEvent) -> ();
        }
    }
}
