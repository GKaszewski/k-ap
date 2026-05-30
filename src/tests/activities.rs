/// Business-logic tests for activity receive() implementations.
///
/// These tests exercise each activity handler with in-memory stubs,
/// verifying the correct callbacks fire and the correct repo mutations happen.
/// Activities that require outbound HTTP (Follow → dereference actor) are tested
/// only for their early-return paths; the happy path requires a real HTTP stack.
use std::collections::HashSet;
use std::sync::Arc;

use activitypub_federation::{config::FederationConfig, fetch::object_id::ObjectId};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use tokio::sync::Mutex;
use url::Url;

use crate::activities::{
    AcceptActivity, AddActivity, AnnounceActivity, AnnounceType, BlockActivity, BlockType,
    CreateActivity, DeleteActivity, FollowActivity, LikeActivity, LikeType, RejectActivity,
    UndoActivity, UpdateActivity,
};
use crate::content::{ApContentReader, ApObjectHandler};
use crate::data::FederationData;
use crate::repository::{
    ActivityRepository, ActorRepository, BlockedDomain, BlocklistRepository, FollowRepository,
    Follower, FollowerStatus, FollowingStatus, RemoteActor,
};
use crate::user::{ApActorType, ApUser, ApUserRepository};

// ── Stubs ─────────────────────────────────────────────────────────────────────

#[derive(Default)]
struct MemActivityRepo {
    processed: Mutex<HashSet<String>>,
}

#[async_trait]
impl ActivityRepository for MemActivityRepo {
    async fn is_activity_processed(&self, id: &str) -> anyhow::Result<bool> {
        Ok(self.processed.lock().await.contains(id))
    }
    async fn mark_activity_processed(&self, id: &str) -> anyhow::Result<()> {
        self.processed.lock().await.insert(id.to_string());
        Ok(())
    }
}

/// Tracking follow repo — records every mutating call for assertion.
#[derive(Default)]
struct MemFollowRepo {
    // recorded mutations
    added_followers: Mutex<Vec<(uuid::Uuid, String, FollowerStatus)>>,
    removed_followers: Mutex<Vec<(uuid::Uuid, String)>>,
    removed_following: Mutex<Vec<(uuid::Uuid, String)>>,
    following_status_updates: Mutex<Vec<(uuid::Uuid, String, FollowingStatus)>>,
}

#[async_trait]
impl FollowRepository for MemFollowRepo {
    async fn add_follower(
        &self,
        local_user_id: uuid::Uuid,
        remote_actor_url: &str,
        status: FollowerStatus,
        _: &str,
    ) -> anyhow::Result<()> {
        self.added_followers.lock().await.push((
            local_user_id,
            remote_actor_url.to_string(),
            status,
        ));
        Ok(())
    }
    async fn get_follower_follow_activity_id(
        &self,
        _: uuid::Uuid,
        _: &str,
    ) -> anyhow::Result<Option<String>> {
        Ok(None)
    }
    async fn remove_follower(
        &self,
        local_user_id: uuid::Uuid,
        remote_actor_url: &str,
    ) -> anyhow::Result<()> {
        self.removed_followers
            .lock()
            .await
            .push((local_user_id, remote_actor_url.to_string()));
        Ok(())
    }
    async fn get_followers(&self, _: uuid::Uuid) -> anyhow::Result<Vec<Follower>> {
        Ok(vec![])
    }
    async fn get_followers_page(
        &self,
        _: uuid::Uuid,
        _: u32,
        _: usize,
    ) -> anyhow::Result<Vec<Follower>> {
        Ok(vec![])
    }
    async fn count_followers(&self, _: uuid::Uuid) -> anyhow::Result<usize> {
        Ok(0)
    }
    async fn update_follower_status(
        &self,
        _: uuid::Uuid,
        _: &str,
        _: FollowerStatus,
    ) -> anyhow::Result<()> {
        Ok(())
    }
    async fn get_pending_followers(&self, _: uuid::Uuid) -> anyhow::Result<Vec<RemoteActor>> {
        Ok(vec![])
    }
    async fn get_accepted_follower_inboxes(&self, _: uuid::Uuid) -> anyhow::Result<Vec<String>> {
        Ok(vec![])
    }
    async fn count_accepted_followers(&self, _: uuid::Uuid) -> anyhow::Result<usize> {
        Ok(0)
    }
    async fn get_accepted_followers_page(
        &self,
        _: uuid::Uuid,
        _: u32,
        _: usize,
    ) -> anyhow::Result<Vec<RemoteActor>> {
        Ok(vec![])
    }
    async fn add_following(&self, _: uuid::Uuid, _: RemoteActor, _: &str) -> anyhow::Result<()> {
        Ok(())
    }
    async fn get_follow_activity_id(
        &self,
        _: uuid::Uuid,
        _: &str,
    ) -> anyhow::Result<Option<String>> {
        Ok(None)
    }
    async fn remove_following(
        &self,
        local_user_id: uuid::Uuid,
        actor_url: &str,
    ) -> anyhow::Result<()> {
        self.removed_following
            .lock()
            .await
            .push((local_user_id, actor_url.to_string()));
        Ok(())
    }
    async fn get_following(&self, _: uuid::Uuid) -> anyhow::Result<Vec<RemoteActor>> {
        Ok(vec![])
    }
    async fn get_following_page(
        &self,
        _: uuid::Uuid,
        _: u32,
        _: usize,
    ) -> anyhow::Result<Vec<RemoteActor>> {
        Ok(vec![])
    }
    async fn count_following(&self, _: uuid::Uuid) -> anyhow::Result<usize> {
        Ok(0)
    }
    async fn update_following_status(
        &self,
        local_user_id: uuid::Uuid,
        remote_actor_url: &str,
        status: FollowingStatus,
    ) -> anyhow::Result<()> {
        self.following_status_updates.lock().await.push((
            local_user_id,
            remote_actor_url.to_string(),
            status,
        ));
        Ok(())
    }
    async fn get_following_outbox_url(
        &self,
        _: uuid::Uuid,
        _: &str,
    ) -> anyhow::Result<Option<String>> {
        Ok(None)
    }
    async fn migrate_follower_actor(&self, _: &str, _: &str) -> anyhow::Result<Vec<uuid::Uuid>> {
        Ok(vec![])
    }
}

/// Tracking actor repo.
#[derive(Default)]
struct MemActorRepo {
    added_announces: Mutex<Vec<String>>,
    removed_announces: Mutex<Vec<String>>,
}

#[async_trait]
impl ActorRepository for MemActorRepo {
    async fn get_local_actor_keypair(
        &self,
        _: uuid::Uuid,
    ) -> anyhow::Result<Option<(String, String)>> {
        Ok(None)
    }
    async fn save_local_actor_keypair(
        &self,
        _: uuid::Uuid,
        _: String,
        _: String,
    ) -> anyhow::Result<()> {
        Ok(())
    }
    async fn upsert_remote_actor(&self, _: RemoteActor) -> anyhow::Result<()> {
        Ok(())
    }
    async fn get_remote_actor(&self, _: &str) -> anyhow::Result<Option<RemoteActor>> {
        Ok(None)
    }
    async fn add_announce(
        &self,
        activity_id: &str,
        _: &str,
        _: &str,
        _: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        self.added_announces
            .lock()
            .await
            .push(activity_id.to_string());
        Ok(())
    }
    async fn remove_announce(&self, activity_id: &str, _: &str) -> anyhow::Result<()> {
        self.removed_announces
            .lock()
            .await
            .push(activity_id.to_string());
        Ok(())
    }
    async fn count_announces(&self, _: &str) -> anyhow::Result<usize> {
        Ok(0)
    }
}

struct MemBlocklistRepo {
    blocked_domains: HashSet<String>,
    blocked_actors: HashSet<(uuid::Uuid, String)>,
}

impl MemBlocklistRepo {
    fn blocking_domain(domain: &str) -> Self {
        let mut blocked_domains = HashSet::new();
        blocked_domains.insert(domain.to_string());
        Self {
            blocked_domains,
            blocked_actors: HashSet::new(),
        }
    }
    fn blocking_actor(local_user_id: uuid::Uuid, actor_url: &str) -> Self {
        let mut blocked_actors = HashSet::new();
        blocked_actors.insert((local_user_id, actor_url.to_string()));
        Self {
            blocked_domains: HashSet::new(),
            blocked_actors,
        }
    }
}

impl Default for MemBlocklistRepo {
    fn default() -> Self {
        Self {
            blocked_domains: HashSet::new(),
            blocked_actors: HashSet::new(),
        }
    }
}

#[async_trait]
impl BlocklistRepository for MemBlocklistRepo {
    async fn add_blocked_domain(&self, _: &str, _: Option<&str>) -> anyhow::Result<()> {
        Ok(())
    }
    async fn remove_blocked_domain(&self, _: &str) -> anyhow::Result<()> {
        Ok(())
    }
    async fn get_blocked_domains(&self) -> anyhow::Result<Vec<BlockedDomain>> {
        Ok(vec![])
    }
    async fn is_domain_blocked(&self, domain: &str) -> anyhow::Result<bool> {
        Ok(self.blocked_domains.contains(domain))
    }
    async fn add_blocked_actor(&self, _: uuid::Uuid, _: &str) -> anyhow::Result<()> {
        Ok(())
    }
    async fn remove_blocked_actor(&self, _: uuid::Uuid, _: &str) -> anyhow::Result<()> {
        Ok(())
    }
    async fn get_blocked_actors(&self, _: uuid::Uuid) -> anyhow::Result<Vec<String>> {
        Ok(vec![])
    }
    async fn is_actor_blocked(
        &self,
        local_user_id: uuid::Uuid,
        actor_url: &str,
    ) -> anyhow::Result<bool> {
        Ok(self
            .blocked_actors
            .contains(&(local_user_id, actor_url.to_string())))
    }
}

struct MemUserRepo {
    user_id: uuid::Uuid,
    username: String,
}

impl MemUserRepo {
    fn new(user_id: uuid::Uuid, username: &str) -> Self {
        Self {
            user_id,
            username: username.to_string(),
        }
    }
}

#[async_trait]
impl ApUserRepository for MemUserRepo {
    async fn find_by_id(&self, id: uuid::Uuid) -> anyhow::Result<Option<ApUser>> {
        if id == self.user_id {
            Ok(Some(ApUser {
                id,
                username: self.username.clone(),
                display_name: None,
                bio: None,
                avatar_url: None,
                banner_url: None,
                also_known_as: vec![],
                profile_url: None,
                attachment: vec![],
                manually_approves_followers: true,
                discoverable: true,
                actor_type: ApActorType::Person,
                featured_url: None,
            }))
        } else {
            Ok(None)
        }
    }
    async fn find_by_username(&self, _: &str) -> anyhow::Result<Option<ApUser>> {
        Ok(None)
    }
    async fn count_users(&self) -> anyhow::Result<usize> {
        Ok(1)
    }
}

/// Tracking object handler — records every callback for assertion.
#[derive(Default)]
struct MemHandler {
    creates: Mutex<Vec<(Url, Url)>>, // (ap_id, actor_url)
    updates: Mutex<Vec<(Url, Url)>>,
    deletes: Mutex<Vec<(Url, Url)>>,
    actors_removed: Mutex<Vec<Url>>,
    likes: Mutex<Vec<(Url, Url)>>,
    unlikes: Mutex<Vec<(Url, Url)>>,
    announces_received: Mutex<Vec<(Url, Url)>>,
    announces_removed: Mutex<Vec<(Url, Url)>>,
    announces_of_remote: Mutex<Vec<(Url, Url)>>,
    mentions: Mutex<Vec<(Url, uuid::Uuid)>>,
}

#[async_trait]
impl ApObjectHandler for MemHandler {
    async fn on_create(
        &self,
        ap_id: &Url,
        actor_url: &Url,
        _: serde_json::Value,
    ) -> anyhow::Result<()> {
        self.creates
            .lock()
            .await
            .push((ap_id.clone(), actor_url.clone()));
        Ok(())
    }
    async fn on_update(
        &self,
        ap_id: &Url,
        actor_url: &Url,
        _: serde_json::Value,
    ) -> anyhow::Result<()> {
        self.updates
            .lock()
            .await
            .push((ap_id.clone(), actor_url.clone()));
        Ok(())
    }
    async fn on_delete(&self, ap_id: &Url, actor_url: &Url) -> anyhow::Result<()> {
        self.deletes
            .lock()
            .await
            .push((ap_id.clone(), actor_url.clone()));
        Ok(())
    }
    async fn on_actor_removed(&self, actor_url: &Url) -> anyhow::Result<()> {
        self.actors_removed.lock().await.push(actor_url.clone());
        Ok(())
    }
    async fn on_like(&self, object_url: &Url, actor_url: &Url) -> anyhow::Result<()> {
        self.likes
            .lock()
            .await
            .push((object_url.clone(), actor_url.clone()));
        Ok(())
    }
    async fn on_unlike(&self, object_url: &Url, actor_url: &Url) -> anyhow::Result<()> {
        self.unlikes
            .lock()
            .await
            .push((object_url.clone(), actor_url.clone()));
        Ok(())
    }
    async fn on_announce_received(&self, object_url: &Url, actor_url: &Url) -> anyhow::Result<()> {
        self.announces_received
            .lock()
            .await
            .push((object_url.clone(), actor_url.clone()));
        Ok(())
    }
    async fn on_announce_removed(&self, object_url: &Url, actor_url: &Url) -> anyhow::Result<()> {
        self.announces_removed
            .lock()
            .await
            .push((object_url.clone(), actor_url.clone()));
        Ok(())
    }
    async fn on_announce_of_remote(&self, object_url: &Url, actor_url: &Url) -> anyhow::Result<()> {
        self.announces_of_remote
            .lock()
            .await
            .push((object_url.clone(), actor_url.clone()));
        Ok(())
    }
    async fn on_mention(&self, ap_id: &Url, user_id: uuid::Uuid, _: &Url) -> anyhow::Result<()> {
        self.mentions.lock().await.push((ap_id.clone(), user_id));
        Ok(())
    }
}

#[derive(Default)]
struct MemContentReader;

#[async_trait]
impl ApContentReader for MemContentReader {
    async fn get_local_objects_page(
        &self,
        _: uuid::Uuid,
        _: Option<DateTime<Utc>>,
        _: usize,
    ) -> anyhow::Result<Vec<(Url, serde_json::Value, DateTime<Utc>)>> {
        Ok(vec![])
    }
    async fn count_local_posts(&self) -> anyhow::Result<u64> {
        Ok(0)
    }
}

// ── Test helpers ──────────────────────────────────────────────────────────────

const LOCAL_DOMAIN: &str = "example.com";
const BASE_URL: &str = "https://example.com";
const REMOTE_ACTOR: &str = "https://remote.example/users/bob";

fn local_user_id() -> uuid::Uuid {
    uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_URL, b"alice")
}

fn local_actor_url() -> Url {
    format!("{}/users/{}", BASE_URL, local_user_id())
        .parse()
        .unwrap()
}

fn remote_actor_url() -> Url {
    REMOTE_ACTOR.parse().unwrap()
}

fn activity_url(path: &str) -> Url {
    format!("https://remote.example{}", path).parse().unwrap()
}

fn local_note_url() -> Url {
    format!("{}/notes/1", BASE_URL).parse().unwrap()
}

fn remote_note_url() -> Url {
    "https://other.example/notes/99".parse().unwrap()
}

struct TestSetup {
    follow_repo: Arc<MemFollowRepo>,
    actor_repo: Arc<MemActorRepo>,
    handler: Arc<MemHandler>,
    config: FederationConfig<FederationData>,
}

async fn setup(blocklist: MemBlocklistRepo, local_user_id: uuid::Uuid) -> TestSetup {
    let follow_repo = Arc::new(MemFollowRepo::default());
    let actor_repo = Arc::new(MemActorRepo::default());
    let handler = Arc::new(MemHandler::default());

    let data = FederationData::new(
        Arc::new(MemActivityRepo::default()),
        follow_repo.clone(),
        actor_repo.clone(),
        Arc::new(blocklist),
        Arc::new(MemUserRepo::new(local_user_id, "alice")),
        Arc::new(MemContentReader),
        handler.clone(),
        BASE_URL.to_string(),
        false,
        "test".to_string(),
        None,
        std::time::Duration::from_secs(24 * 60 * 60),
    );

    let config = FederationConfig::builder()
        .domain(LOCAL_DOMAIN)
        .app_data(data)
        .debug(true)
        .build()
        .await
        .unwrap();

    TestSetup {
        follow_repo,
        actor_repo,
        handler,
        config,
    }
}

// ── AcceptActivity tests ───────────────────────────────────────────────────────

#[tokio::test]
async fn accept_updates_following_status_to_accepted() {
    use activitypub_federation::kinds::activity::AcceptType;

    let local_id = local_user_id();
    let s = setup(MemBlocklistRepo::default(), local_id).await;
    let data = s.config.to_request_data();

    // AP Accept flow:
    //   Our local actor sent Follow(remote_actor).
    //   Remote actor responds with Accept(Follow(...)).
    //
    // FollowActivity.actor  = our local actor (who sent the Follow)
    // FollowActivity.object = the remote actor we followed
    // AcceptActivity.actor  = the remote actor (who accepted)
    // AcceptActivity.object = the original FollowActivity
    //
    // AcceptActivity::receive extracts local_user_id from FollowActivity.actor,
    // so that URL must be UUID-based (our standard actor URL format).
    let follow = FollowActivity {
        id: activity_url("/follow/1"),
        kind: Default::default(),
        actor: ObjectId::from(local_actor_url()), // OUR actor sent the Follow
        object: ObjectId::from(remote_actor_url()), // we followed remote
    };
    let accept = AcceptActivity {
        id: activity_url("/accept/1"),
        kind: AcceptType::default(),
        actor: ObjectId::from(remote_actor_url()), // remote accepts
        object: follow,
    };

    use activitypub_federation::traits::Activity;
    accept.receive(&data).await.unwrap();

    let updates = s.follow_repo.following_status_updates.lock().await;
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].0, local_id);
    assert_eq!(updates[0].1, REMOTE_ACTOR);
    assert!(matches!(updates[0].2, FollowingStatus::Accepted));
}

// ── RejectActivity tests ───────────────────────────────────────────────────────

#[tokio::test]
async fn reject_removes_following() {
    use activitypub_federation::kinds::activity::RejectType;

    let local_id = local_user_id();
    let s = setup(MemBlocklistRepo::default(), local_id).await;
    let data = s.config.to_request_data();

    // Same actor structure as Accept: local sent Follow(remote), remote rejects.
    // RejectActivity::receive extracts local_user_id from FollowActivity.actor.
    let follow = FollowActivity {
        id: activity_url("/follow/1"),
        kind: Default::default(),
        actor: ObjectId::from(local_actor_url()), // OUR actor sent the Follow
        object: ObjectId::from(remote_actor_url()), // we followed remote
    };
    let reject = RejectActivity {
        id: activity_url("/reject/1"),
        kind: RejectType::default(),
        actor: ObjectId::from(remote_actor_url()),
        object: follow,
    };

    use activitypub_federation::traits::Activity;
    reject.receive(&data).await.unwrap();

    let removed = s.follow_repo.removed_following.lock().await;
    assert_eq!(removed.len(), 1);
    assert_eq!(removed[0].0, local_id);
    assert_eq!(removed[0].1, REMOTE_ACTOR);
}

// ── UndoActivity tests ────────────────────────────────────────────────────────

#[tokio::test]
async fn undo_follow_removes_follower_and_cleans_content() {
    let local_id = local_user_id();
    let s = setup(MemBlocklistRepo::default(), local_id).await;
    let data = s.config.to_request_data();

    let undo = UndoActivity {
        id: activity_url("/undo/1"),
        kind: Default::default(),
        actor: ObjectId::from(remote_actor_url()),
        object: serde_json::json!({
            "type": "Follow",
            "id": "https://remote.example/follow/1",
            "actor": REMOTE_ACTOR,
            "object": local_actor_url().as_str(),
        }),
    };

    use activitypub_federation::traits::Activity;
    undo.receive(&data).await.unwrap();

    let removed = s.follow_repo.removed_followers.lock().await;
    assert_eq!(removed.len(), 1, "follower should be removed");
    assert_eq!(removed[0].0, local_id);

    let cleaned = s.handler.actors_removed.lock().await;
    assert_eq!(cleaned.len(), 1, "on_actor_removed should be called");
    assert_eq!(cleaned[0], remote_actor_url());
}

#[tokio::test]
async fn undo_like_calls_on_unlike_for_local_object() {
    let local_id = local_user_id();
    let s = setup(MemBlocklistRepo::default(), local_id).await;
    let data = s.config.to_request_data();

    let undo = UndoActivity {
        id: activity_url("/undo/2"),
        kind: Default::default(),
        actor: ObjectId::from(remote_actor_url()),
        object: serde_json::json!({
            "type": "Like",
            "id": "https://remote.example/like/1",
            "actor": REMOTE_ACTOR,
            "object": local_note_url().as_str(),
        }),
    };

    use activitypub_federation::traits::Activity;
    undo.receive(&data).await.unwrap();

    let unlikes = s.handler.unlikes.lock().await;
    assert_eq!(unlikes.len(), 1);
    assert_eq!(unlikes[0].0, local_note_url());
}

#[tokio::test]
async fn undo_like_ignores_remote_object() {
    let local_id = local_user_id();
    let s = setup(MemBlocklistRepo::default(), local_id).await;
    let data = s.config.to_request_data();

    let undo = UndoActivity {
        id: activity_url("/undo/3"),
        kind: Default::default(),
        actor: ObjectId::from(remote_actor_url()),
        object: serde_json::json!({
            "type": "Like",
            "id": "https://remote.example/like/2",
            "actor": REMOTE_ACTOR,
            "object": remote_note_url().as_str(),  // NOT local
        }),
    };

    use activitypub_federation::traits::Activity;
    undo.receive(&data).await.unwrap();

    let unlikes = s.handler.unlikes.lock().await;
    assert!(
        unlikes.is_empty(),
        "remote object Like should not trigger on_unlike"
    );
}

#[tokio::test]
async fn undo_announce_removes_record_and_notifies() {
    let local_id = local_user_id();
    let s = setup(MemBlocklistRepo::default(), local_id).await;
    let data = s.config.to_request_data();

    let undo = UndoActivity {
        id: activity_url("/undo/4"),
        kind: Default::default(),
        actor: ObjectId::from(remote_actor_url()),
        object: serde_json::json!({
            "type": "Announce",
            "id": "https://remote.example/announce/1",
            "actor": REMOTE_ACTOR,
            "object": local_note_url().as_str(),
        }),
    };

    use activitypub_federation::traits::Activity;
    undo.receive(&data).await.unwrap();

    let removed = s.actor_repo.removed_announces.lock().await;
    assert_eq!(removed.len(), 1);
    assert_eq!(removed[0], "https://remote.example/announce/1");

    let notified = s.handler.announces_removed.lock().await;
    assert_eq!(notified.len(), 1);
    assert_eq!(notified[0].0, local_note_url());
}

#[tokio::test]
async fn undo_announce_ignores_remote_object() {
    let local_id = local_user_id();
    let s = setup(MemBlocklistRepo::default(), local_id).await;
    let data = s.config.to_request_data();

    let undo = UndoActivity {
        id: activity_url("/undo/5"),
        kind: Default::default(),
        actor: ObjectId::from(remote_actor_url()),
        object: serde_json::json!({
            "type": "Announce",
            "id": "https://remote.example/announce/2",
            "actor": REMOTE_ACTOR,
            "object": remote_note_url().as_str(),   // NOT local
        }),
    };

    use activitypub_federation::traits::Activity;
    undo.receive(&data).await.unwrap();

    // remove_announce should still be called (clean up the record)
    let removed = s.actor_repo.removed_announces.lock().await;
    assert_eq!(
        removed.len(),
        1,
        "announce record should be removed regardless"
    );

    // but on_announce_removed should NOT fire for non-local objects
    let notified = s.handler.announces_removed.lock().await;
    assert!(
        notified.is_empty(),
        "on_announce_removed should not fire for remote-hosted objects"
    );
}

// ── CreateActivity tests ───────────────────────────────────────────────────────

#[tokio::test]
async fn create_uses_object_id_not_activity_id() {
    let local_id = local_user_id();
    let s = setup(MemBlocklistRepo::default(), local_id).await;
    let data = s.config.to_request_data();

    let object_id = "https://remote.example/notes/42";
    let create = CreateActivity {
        id: activity_url("/create/99"), // activity id — should NOT be used
        kind: Default::default(),
        actor: ObjectId::from(remote_actor_url()),
        object: serde_json::json!({
            "type": "Note",
            "id": object_id,
            "content": "Hello world",
            "attributedTo": REMOTE_ACTOR,
        }),
        to: vec![],
        cc: vec![],
        bto: vec![],
        bcc: vec![],
    };

    use activitypub_federation::traits::Activity;
    create.receive(&data).await.unwrap();

    let creates = s.handler.creates.lock().await;
    assert_eq!(creates.len(), 1);
    assert_eq!(
        creates[0].0.as_str(),
        object_id,
        "on_create should receive the OBJECT id, not the Create activity id"
    );
}

#[tokio::test]
async fn create_with_mention_fires_on_mention() {
    let local_id = local_user_id();
    let s = setup(MemBlocklistRepo::default(), local_id).await;
    let data = s.config.to_request_data();

    let note_id = "https://remote.example/notes/mention-test";
    let local_user_url = local_actor_url();
    let create = CreateActivity {
        id: activity_url("/create/mention"),
        kind: Default::default(),
        actor: ObjectId::from(remote_actor_url()),
        object: serde_json::json!({
            "type": "Note",
            "id": note_id,
            "content": "Hey @alice!",
            "attributedTo": REMOTE_ACTOR,
            "tag": [{"type": "Mention", "href": local_user_url.as_str()}],
        }),
        to: vec![],
        cc: vec![],
        bto: vec![],
        bcc: vec![],
    };

    use activitypub_federation::traits::Activity;
    create.receive(&data).await.unwrap();

    let mentions = s.handler.mentions.lock().await;
    assert_eq!(
        mentions.len(),
        1,
        "on_mention should fire for the local user"
    );
    assert_eq!(mentions[0].1, local_id);

    // on_create should ALSO fire (mention doesn't replace content delivery)
    let creates = s.handler.creates.lock().await;
    assert_eq!(creates.len(), 1);
}

// ── UpdateActivity tests ───────────────────────────────────────────────────────

#[tokio::test]
async fn update_uses_object_id() {
    let local_id = local_user_id();
    let s = setup(MemBlocklistRepo::default(), local_id).await;
    let data = s.config.to_request_data();

    let object_id = "https://remote.example/notes/42";
    let update = UpdateActivity {
        id: activity_url("/update/1"),
        kind: Default::default(),
        actor: ObjectId::from(remote_actor_url()),
        object: serde_json::json!({"type": "Note", "id": object_id, "content": "Edited"}),
        to: vec![],
        cc: vec![],
    };

    use activitypub_federation::traits::Activity;
    update.receive(&data).await.unwrap();

    let updates = s.handler.updates.lock().await;
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].0.as_str(), object_id);
}

// ── DeleteActivity tests ───────────────────────────────────────────────────────

#[tokio::test]
async fn delete_object_calls_on_delete() {
    let local_id = local_user_id();
    let s = setup(MemBlocklistRepo::default(), local_id).await;
    let data = s.config.to_request_data();

    let note_id = "https://remote.example/notes/to-delete";
    let delete = DeleteActivity {
        id: activity_url("/delete/1"),
        kind: Default::default(),
        actor: ObjectId::from(remote_actor_url()),
        object: serde_json::json!({"type": "Tombstone", "id": note_id}),
        to: vec![],
        cc: vec![],
    };

    use activitypub_federation::traits::Activity;
    delete.receive(&data).await.unwrap();

    let deletes = s.handler.deletes.lock().await;
    assert_eq!(deletes.len(), 1);
    assert_eq!(deletes[0].0.as_str(), note_id);

    let actor_removed = s.handler.actors_removed.lock().await;
    assert!(
        actor_removed.is_empty(),
        "on_actor_removed should NOT fire for note deletion"
    );
}

#[tokio::test]
async fn delete_actor_calls_on_actor_removed() {
    let local_id = local_user_id();
    let s = setup(MemBlocklistRepo::default(), local_id).await;
    let data = s.config.to_request_data();

    // AP actor self-deletion: object URL == actor URL
    let delete = DeleteActivity {
        id: activity_url("/delete/actor"),
        kind: Default::default(),
        actor: ObjectId::from(remote_actor_url()),
        object: serde_json::json!(REMOTE_ACTOR), // plain URL string
        to: vec![],
        cc: vec![],
    };

    use activitypub_federation::traits::Activity;
    delete.receive(&data).await.unwrap();

    let actor_removed = s.handler.actors_removed.lock().await;
    assert_eq!(actor_removed.len(), 1);
    assert_eq!(actor_removed[0], remote_actor_url());

    let deletes = s.handler.deletes.lock().await;
    assert!(
        deletes.is_empty(),
        "on_delete should NOT fire for actor deletion"
    );
}

// ── AnnounceActivity tests ────────────────────────────────────────────────────

#[tokio::test]
async fn announce_local_object_records_and_notifies() {
    let local_id = local_user_id();
    let s = setup(MemBlocklistRepo::default(), local_id).await;
    let data = s.config.to_request_data();

    let announce = AnnounceActivity {
        id: activity_url("/announce/1"),
        kind: AnnounceType,
        actor: ObjectId::from(remote_actor_url()),
        object: local_note_url(),
        published: None,
        to: vec![],
        cc: vec![],
    };

    use activitypub_federation::traits::Activity;
    announce.receive(&data).await.unwrap();

    let added = s.actor_repo.added_announces.lock().await;
    assert_eq!(added.len(), 1, "announce record should be created");

    let notified = s.handler.announces_received.lock().await;
    assert_eq!(notified.len(), 1);
    assert_eq!(notified[0].0, local_note_url());

    let remote = s.handler.announces_of_remote.lock().await;
    assert!(
        remote.is_empty(),
        "on_announce_of_remote should NOT fire for local objects"
    );
}

#[tokio::test]
async fn announce_remote_object_calls_on_announce_of_remote() {
    let local_id = local_user_id();
    let s = setup(MemBlocklistRepo::default(), local_id).await;
    let data = s.config.to_request_data();

    let announce = AnnounceActivity {
        id: activity_url("/announce/2"),
        kind: AnnounceType,
        actor: ObjectId::from(remote_actor_url()),
        object: remote_note_url(), // NOT local
        published: None,
        to: vec![],
        cc: vec![],
    };

    use activitypub_federation::traits::Activity;
    announce.receive(&data).await.unwrap();

    let remote = s.handler.announces_of_remote.lock().await;
    assert_eq!(remote.len(), 1);
    assert_eq!(remote[0].0, remote_note_url());

    let local = s.handler.announces_received.lock().await;
    assert!(
        local.is_empty(),
        "on_announce_received should NOT fire for remote objects"
    );

    let added = s.actor_repo.added_announces.lock().await;
    assert!(
        added.is_empty(),
        "announce record should NOT be created for remote objects"
    );
}

// ── LikeActivity tests ────────────────────────────────────────────────────────

#[tokio::test]
async fn like_local_object_calls_on_like() {
    let local_id = local_user_id();
    let s = setup(MemBlocklistRepo::default(), local_id).await;
    let data = s.config.to_request_data();

    let like = LikeActivity {
        id: activity_url("/like/1"),
        kind: LikeType,
        actor: ObjectId::from(remote_actor_url()),
        object: local_note_url(),
    };

    use activitypub_federation::traits::Activity;
    like.receive(&data).await.unwrap();

    let likes = s.handler.likes.lock().await;
    assert_eq!(likes.len(), 1);
    assert_eq!(likes[0].0, local_note_url());
}

#[tokio::test]
async fn like_remote_object_is_ignored() {
    let local_id = local_user_id();
    let s = setup(MemBlocklistRepo::default(), local_id).await;
    let data = s.config.to_request_data();

    let like = LikeActivity {
        id: activity_url("/like/2"),
        kind: LikeType,
        actor: ObjectId::from(remote_actor_url()),
        object: remote_note_url(), // NOT local
    };

    use activitypub_federation::traits::Activity;
    like.receive(&data).await.unwrap();

    let likes = s.handler.likes.lock().await;
    assert!(
        likes.is_empty(),
        "remote object Like should be silently ignored"
    );
}

// ── AddActivity tests ─────────────────────────────────────────────────────────

#[tokio::test]
async fn add_uses_object_id_not_activity_id() {
    let local_id = local_user_id();
    let s = setup(MemBlocklistRepo::default(), local_id).await;
    let data = s.config.to_request_data();

    let object_id = "https://remote.example/watchlist/item/5";
    let add = AddActivity {
        id: activity_url("/add/99"), // activity id — should NOT be used
        kind: Default::default(),
        actor: ObjectId::from(remote_actor_url()),
        object: serde_json::json!({
            "type": "Movie",
            "id": object_id,
            "name": "Some Film",
            "attributedTo": REMOTE_ACTOR,
        }),
        to: vec![],
        cc: vec![],
    };

    use activitypub_federation::traits::Activity;
    add.receive(&data).await.unwrap();

    let creates = s.handler.creates.lock().await;
    assert_eq!(creates.len(), 1);
    assert_eq!(
        creates[0].0.as_str(),
        object_id,
        "on_create should use the OBJECT id, not the Add activity id"
    );
}

// ── BlockActivity tests ───────────────────────────────────────────────────────

#[tokio::test]
async fn block_removes_follow_relationships() {
    let local_id = local_user_id();
    let s = setup(MemBlocklistRepo::default(), local_id).await;
    let data = s.config.to_request_data();

    let block = BlockActivity {
        id: activity_url("/block/1"),
        kind: BlockType,
        actor: ObjectId::from(remote_actor_url()),
        object: local_actor_url(),
    };

    use activitypub_federation::traits::Activity;
    block.receive(&data).await.unwrap();

    let removed_following = s.follow_repo.removed_following.lock().await;
    assert_eq!(
        removed_following.len(),
        1,
        "following should be removed on Block"
    );
    assert_eq!(removed_following[0].0, local_id);

    let removed_followers = s.follow_repo.removed_followers.lock().await;
    assert_eq!(
        removed_followers.len(),
        1,
        "follower should be removed on Block"
    );
    assert_eq!(removed_followers[0].0, local_id);
}

// ── Domain / actor blocking ───────────────────────────────────────────────────

#[tokio::test]
async fn activity_from_blocked_domain_is_skipped() {
    let local_id = local_user_id();
    let s = setup(
        MemBlocklistRepo::blocking_domain("remote.example"),
        local_id,
    )
    .await;
    let data = s.config.to_request_data();

    let create = CreateActivity {
        id: activity_url("/create/blocked"),
        kind: Default::default(),
        actor: ObjectId::from(remote_actor_url()),
        object: serde_json::json!({"type": "Note", "id": "https://remote.example/notes/1"}),
        to: vec![],
        cc: vec![],
        bto: vec![],
        bcc: vec![],
    };

    use activitypub_federation::traits::Activity;
    create.receive(&data).await.unwrap();

    let creates = s.handler.creates.lock().await;
    assert!(
        creates.is_empty(),
        "activity from blocked domain must be skipped"
    );
}

#[tokio::test]
async fn follow_from_blocked_actor_is_skipped_before_http() {
    // Verifies the SSRF fix: actor block checked BEFORE any HTTP dereference.
    let local_id = local_user_id();
    let s = setup(
        MemBlocklistRepo::blocking_actor(local_id, REMOTE_ACTOR),
        local_id,
    )
    .await;
    let data = s.config.to_request_data();

    let follow = FollowActivity {
        id: activity_url("/follow/blocked"),
        kind: Default::default(),
        actor: ObjectId::from(remote_actor_url()),
        object: ObjectId::from(local_actor_url()),
    };

    use activitypub_federation::traits::Activity;
    // In debug mode, dereference would be attempted if we got past the block check.
    // The test passes only if we return early before any HTTP call.
    follow.receive(&data).await.unwrap();

    let added = s.follow_repo.added_followers.lock().await;
    assert!(
        added.is_empty(),
        "blocked actor follow must be silently discarded"
    );
}

// ── Idempotency ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn duplicate_activity_id_is_skipped() {
    let local_id = local_user_id();
    let s = setup(MemBlocklistRepo::default(), local_id).await;
    let data = s.config.to_request_data();

    let make_create = || CreateActivity {
        id: activity_url("/create/dedup"),
        kind: Default::default(),
        actor: ObjectId::from(remote_actor_url()),
        object: serde_json::json!({"type": "Note", "id": "https://remote.example/notes/dedup"}),
        to: vec![],
        cc: vec![],
        bto: vec![],
        bcc: vec![],
    };

    use activitypub_federation::traits::Activity;
    make_create().receive(&data).await.unwrap();
    make_create().receive(&data).await.unwrap(); // duplicate

    let creates = s.handler.creates.lock().await;
    assert_eq!(creates.len(), 1, "duplicate delivery must be deduplicated");
}
