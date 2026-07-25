/// Business-logic tests for activity receive() implementations.
///
/// These tests exercise each activity handler with mock builders,
/// verifying the correct callbacks fire and the correct repo mutations happen.
/// Activities that require outbound HTTP (Follow -> dereference actor) are tested
/// only for their early-return paths; the happy path requires a real HTTP stack.
use std::collections::HashSet;
use std::sync::Arc;

use activitypub_federation::{config::FederationConfig, fetch::object_id::ObjectId};
use tokio::sync::Mutex;
use url::Url;

use crate::activities::announce::AnnounceType;
use crate::activities::block::BlockType;
use crate::activities::like::LikeType;
use crate::activities::{
    AcceptActivity, AddActivity, AnnounceActivity, BlockActivity, CreateActivity, DeleteActivity,
    FollowActivity, LikeActivity, RejectActivity, UndoActivity, UpdateActivity,
};
use crate::data::FederationData;
use crate::repository::FollowingStatus;
use crate::testing::{
    MockActivityRepoBuilder, MockActorRepoBuilder, MockBlocklistRepoBuilder,
    MockContentReaderBuilder, MockFollowRepoBuilder, MockObjectHandlerBuilder, MockUserRepoBuilder,
};
use crate::user::{ApActorType, ApUser};

// ── Mock-builder-based tracking ─────────────────────────────────────────────

fn make_user(id: uuid::Uuid, username: &str) -> ApUser {
    ApUser {
        id,
        username: username.to_string(),
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
    }
}

/// Tracking vectors for object handler callbacks.
#[derive(Default)]
struct HandlerTracking {
    creates: Arc<Mutex<Vec<(Url, Url)>>>,
    updates: Arc<Mutex<Vec<(Url, Url)>>>,
    deletes: Arc<Mutex<Vec<(Url, Url)>>>,
    actors_removed: Arc<Mutex<Vec<Url>>>,
    likes: Arc<Mutex<Vec<(Url, Url)>>>,
    unlikes: Arc<Mutex<Vec<(Url, Url)>>>,
    announces_received: Arc<Mutex<Vec<(Url, Url)>>>,
    announces_removed: Arc<Mutex<Vec<(Url, Url)>>>,
    announces_of_remote: Arc<Mutex<Vec<(Url, Url)>>>,
    mentions: Arc<Mutex<Vec<(Url, uuid::Uuid)>>>,
}

impl HandlerTracking {
    fn build_handler(&self) -> Arc<crate::testing::MockObjectHandler> {
        let creates = self.creates.clone();
        let updates = self.updates.clone();
        let deletes = self.deletes.clone();
        let actors_removed = self.actors_removed.clone();
        let likes = self.likes.clone();
        let unlikes = self.unlikes.clone();
        let announces_received = self.announces_received.clone();
        let announces_removed = self.announces_removed.clone();
        let announces_of_remote = self.announces_of_remote.clone();
        let mentions = self.mentions.clone();

        MockObjectHandlerBuilder::new()
            .on_on_create(move |ap_id, actor_url, _| {
                creates
                    .try_lock()
                    .unwrap()
                    .push((ap_id.clone(), actor_url.clone()));
                Ok(())
            })
            .on_on_update(move |ap_id, actor_url, _| {
                updates
                    .try_lock()
                    .unwrap()
                    .push((ap_id.clone(), actor_url.clone()));
                Ok(())
            })
            .on_on_delete(move |ap_id, actor_url| {
                deletes
                    .try_lock()
                    .unwrap()
                    .push((ap_id.clone(), actor_url.clone()));
                Ok(())
            })
            .on_on_actor_removed(move |actor_url| {
                actors_removed.try_lock().unwrap().push(actor_url.clone());
                Ok(())
            })
            .on_on_like(move |object_url, actor_url| {
                likes
                    .try_lock()
                    .unwrap()
                    .push((object_url.clone(), actor_url.clone()));
                Ok(())
            })
            .on_on_unlike(move |object_url, actor_url| {
                unlikes
                    .try_lock()
                    .unwrap()
                    .push((object_url.clone(), actor_url.clone()));
                Ok(())
            })
            .on_on_announce_received(move |object_url, actor_url| {
                announces_received
                    .try_lock()
                    .unwrap()
                    .push((object_url.clone(), actor_url.clone()));
                Ok(())
            })
            .on_on_announce_removed(move |object_url, actor_url| {
                announces_removed
                    .try_lock()
                    .unwrap()
                    .push((object_url.clone(), actor_url.clone()));
                Ok(())
            })
            .on_on_announce_of_remote(move |object_url, actor_url| {
                announces_of_remote
                    .try_lock()
                    .unwrap()
                    .push((object_url.clone(), actor_url.clone()));
                Ok(())
            })
            .on_on_mention(move |ap_id, user_id, _| {
                mentions.try_lock().unwrap().push((ap_id.clone(), user_id));
                Ok(())
            })
            .build()
    }
}

// ── Test helpers ─────────────────────────────────────────────────────────────

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

/// Shared tracking vectors used by closures in mock builders.
struct Tracking {
    added_followers: Arc<Mutex<Vec<(uuid::Uuid, String, crate::repository::FollowerStatus)>>>,
    removed_followers: Arc<Mutex<Vec<(uuid::Uuid, String)>>>,
    removed_following: Arc<Mutex<Vec<(uuid::Uuid, String)>>>,
    following_status_updates: Arc<Mutex<Vec<(uuid::Uuid, String, FollowingStatus)>>>,
    added_announces: Arc<Mutex<Vec<String>>>,
    removed_announces: Arc<Mutex<Vec<String>>>,
}

struct TestSetup {
    tracking: Tracking,
    handler: HandlerTracking,
    config: FederationConfig<FederationData>,
}

async fn setup_with_blocklist(
    blocklist: Arc<crate::testing::MockBlocklistRepo>,
    local_user_id: uuid::Uuid,
) -> TestSetup {
    let added_followers = Arc::new(Mutex::new(vec![]));
    let removed_followers = Arc::new(Mutex::new(vec![]));
    let removed_following = Arc::new(Mutex::new(vec![]));
    let following_status_updates = Arc::new(Mutex::new(vec![]));
    let added_announces = Arc::new(Mutex::new(vec![]));
    let removed_announces = Arc::new(Mutex::new(vec![]));

    let af = added_followers.clone();
    let rf = removed_followers.clone();
    let rfw = removed_following.clone();
    let fsu = following_status_updates.clone();
    let follow_repo = MockFollowRepoBuilder::new()
        .on_add_follower(move |id, url, status, _| {
            af.try_lock().unwrap().push((id, url.to_string(), status));
            Ok(())
        })
        .on_remove_follower(move |id, url| {
            rf.try_lock().unwrap().push((id, url.to_string()));
            Ok(())
        })
        .on_remove_following(move |id, url| {
            rfw.try_lock().unwrap().push((id, url.to_string()));
            Ok(())
        })
        .on_update_following_status(move |id, url, status| {
            fsu.try_lock().unwrap().push((id, url.to_string(), status));
            Ok(())
        })
        .build();

    let aa = added_announces.clone();
    let ra = removed_announces.clone();
    let actor_repo = MockActorRepoBuilder::new()
        .on_add_announce(move |activity_id, _, _, _| {
            aa.try_lock().unwrap().push(activity_id.to_string());
            Ok(())
        })
        .on_remove_announce(move |activity_id, _| {
            ra.try_lock().unwrap().push(activity_id.to_string());
            Ok(())
        })
        .build();

    // Activity repo with real dedup tracking
    let processed = Arc::new(Mutex::new(HashSet::<String>::new()));
    let p1 = processed.clone();
    let p2 = processed.clone();
    let activity_repo = MockActivityRepoBuilder::new()
        .on_is_activity_processed(move |id| Ok(p1.try_lock().unwrap().contains(id)))
        .on_mark_activity_processed(move |id| {
            p2.try_lock().unwrap().insert(id.to_string());
            Ok(())
        })
        .build();

    let handler = HandlerTracking::default();

    let user = make_user(local_user_id, "alice");
    let user_repo = MockUserRepoBuilder::new()
        .on_find_by_id(move |id| {
            if id == local_user_id {
                Ok(Some(user.clone()))
            } else {
                Ok(None)
            }
        })
        .build();

    let data = FederationData::new(
        activity_repo,
        follow_repo,
        actor_repo,
        blocklist,
        user_repo,
        MockContentReaderBuilder::new().build(),
        handler.build_handler(),
        BASE_URL.to_string(),
        false,
        "test".to_string(),
        None,
        std::time::Duration::from_secs(24 * 60 * 60),
        Arc::new(crate::url_scheme::DefaultUrlScheme),
    );

    let config = FederationConfig::builder()
        .domain(LOCAL_DOMAIN)
        .app_data(data)
        .debug(true)
        .build()
        .await
        .unwrap();

    TestSetup {
        tracking: Tracking {
            added_followers,
            removed_followers,
            removed_following,
            following_status_updates,
            added_announces,
            removed_announces,
        },
        handler,
        config,
    }
}

async fn setup(local_user_id: uuid::Uuid) -> TestSetup {
    setup_with_blocklist(MockBlocklistRepoBuilder::new().build(), local_user_id).await
}

// ── AcceptActivity tests ─────────────────────────────────────────────────────

#[tokio::test]
async fn accept_updates_following_status_to_accepted() {
    use activitypub_federation::kinds::activity::AcceptType;

    let local_id = local_user_id();
    let s = setup(local_id).await;
    let data = s.config.to_request_data();

    let follow = FollowActivity {
        id: activity_url("/follow/1"),
        kind: Default::default(),
        actor: ObjectId::from(local_actor_url()),
        object: ObjectId::from(remote_actor_url()),
    };
    let accept = AcceptActivity {
        id: activity_url("/accept/1"),
        kind: AcceptType::default(),
        actor: ObjectId::from(remote_actor_url()),
        object: follow,
    };

    use activitypub_federation::traits::Activity;
    accept.receive(&data).await.unwrap();

    let updates = s.tracking.following_status_updates.lock().await;
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].0, local_id);
    assert_eq!(updates[0].1, REMOTE_ACTOR);
    assert!(matches!(updates[0].2, FollowingStatus::Accepted));
}

// ── RejectActivity tests ─────────────────────────────────────────────────────

#[tokio::test]
async fn reject_removes_following() {
    use activitypub_federation::kinds::activity::RejectType;

    let local_id = local_user_id();
    let s = setup(local_id).await;
    let data = s.config.to_request_data();

    let follow = FollowActivity {
        id: activity_url("/follow/1"),
        kind: Default::default(),
        actor: ObjectId::from(local_actor_url()),
        object: ObjectId::from(remote_actor_url()),
    };
    let reject = RejectActivity {
        id: activity_url("/reject/1"),
        kind: RejectType::default(),
        actor: ObjectId::from(remote_actor_url()),
        object: follow,
    };

    use activitypub_federation::traits::Activity;
    reject.receive(&data).await.unwrap();

    let removed = s.tracking.removed_following.lock().await;
    assert_eq!(removed.len(), 1);
    assert_eq!(removed[0].0, local_id);
    assert_eq!(removed[0].1, REMOTE_ACTOR);
}

// ── UndoActivity tests ──────────────────────────────────────────────────────

#[tokio::test]
async fn undo_follow_removes_follower_and_cleans_content() {
    let local_id = local_user_id();
    let s = setup(local_id).await;
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

    let removed = s.tracking.removed_followers.lock().await;
    assert_eq!(removed.len(), 1, "follower should be removed");
    assert_eq!(removed[0].0, local_id);

    let cleaned = s.handler.actors_removed.lock().await;
    assert_eq!(cleaned.len(), 1, "on_actor_removed should be called");
    assert_eq!(cleaned[0], remote_actor_url());
}

#[tokio::test]
async fn undo_like_calls_on_unlike_for_local_object() {
    let local_id = local_user_id();
    let s = setup(local_id).await;
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
    let s = setup(local_id).await;
    let data = s.config.to_request_data();

    let undo = UndoActivity {
        id: activity_url("/undo/3"),
        kind: Default::default(),
        actor: ObjectId::from(remote_actor_url()),
        object: serde_json::json!({
            "type": "Like",
            "id": "https://remote.example/like/2",
            "actor": REMOTE_ACTOR,
            "object": remote_note_url().as_str(),
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
    let s = setup(local_id).await;
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

    let removed = s.tracking.removed_announces.lock().await;
    assert_eq!(removed.len(), 1);
    assert_eq!(removed[0], "https://remote.example/announce/1");

    let notified = s.handler.announces_removed.lock().await;
    assert_eq!(notified.len(), 1);
    assert_eq!(notified[0].0, local_note_url());
}

#[tokio::test]
async fn undo_announce_ignores_remote_object() {
    let local_id = local_user_id();
    let s = setup(local_id).await;
    let data = s.config.to_request_data();

    let undo = UndoActivity {
        id: activity_url("/undo/5"),
        kind: Default::default(),
        actor: ObjectId::from(remote_actor_url()),
        object: serde_json::json!({
            "type": "Announce",
            "id": "https://remote.example/announce/2",
            "actor": REMOTE_ACTOR,
            "object": remote_note_url().as_str(),
        }),
    };

    use activitypub_federation::traits::Activity;
    undo.receive(&data).await.unwrap();

    // remove_announce should still be called (clean up the record)
    let removed = s.tracking.removed_announces.lock().await;
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

// ── CreateActivity tests ─────────────────────────────────────────────────────

#[tokio::test]
async fn create_uses_object_id_not_activity_id() {
    let local_id = local_user_id();
    let s = setup(local_id).await;
    let data = s.config.to_request_data();

    let object_id = "https://remote.example/notes/42";
    let create = CreateActivity {
        id: activity_url("/create/99"),
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
    let s = setup(local_id).await;
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

    let creates = s.handler.creates.lock().await;
    assert_eq!(creates.len(), 1);
}

// ── UpdateActivity tests ─────────────────────────────────────────────────────

#[tokio::test]
async fn update_uses_object_id() {
    let local_id = local_user_id();
    let s = setup(local_id).await;
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

// ── DeleteActivity tests ─────────────────────────────────────────────────────

#[tokio::test]
async fn delete_object_calls_on_delete() {
    let local_id = local_user_id();
    let s = setup(local_id).await;
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
    let s = setup(local_id).await;
    let data = s.config.to_request_data();

    let delete = DeleteActivity {
        id: activity_url("/delete/actor"),
        kind: Default::default(),
        actor: ObjectId::from(remote_actor_url()),
        object: serde_json::json!(REMOTE_ACTOR),
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

// ── AnnounceActivity tests ───────────────────────────────────────────────────

#[tokio::test]
async fn announce_local_object_records_and_notifies() {
    let local_id = local_user_id();
    let s = setup(local_id).await;
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

    let added = s.tracking.added_announces.lock().await;
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
    let s = setup(local_id).await;
    let data = s.config.to_request_data();

    let announce = AnnounceActivity {
        id: activity_url("/announce/2"),
        kind: AnnounceType,
        actor: ObjectId::from(remote_actor_url()),
        object: remote_note_url(),
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

    let added = s.tracking.added_announces.lock().await;
    assert!(
        added.is_empty(),
        "announce record should NOT be created for remote objects"
    );
}

// ── LikeActivity tests ──────────────────────────────────────────────────────

#[tokio::test]
async fn like_local_object_calls_on_like() {
    let local_id = local_user_id();
    let s = setup(local_id).await;
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
    let s = setup(local_id).await;
    let data = s.config.to_request_data();

    let like = LikeActivity {
        id: activity_url("/like/2"),
        kind: LikeType,
        actor: ObjectId::from(remote_actor_url()),
        object: remote_note_url(),
    };

    use activitypub_federation::traits::Activity;
    like.receive(&data).await.unwrap();

    let likes = s.handler.likes.lock().await;
    assert!(
        likes.is_empty(),
        "remote object Like should be silently ignored"
    );
}

// ── AddActivity tests ────────────────────────────────────────────────────────

#[tokio::test]
async fn add_uses_object_id_not_activity_id() {
    let local_id = local_user_id();
    let s = setup(local_id).await;
    let data = s.config.to_request_data();

    let object_id = "https://remote.example/watchlist/item/5";
    let add = AddActivity {
        id: activity_url("/add/99"),
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

// ── BlockActivity tests ─────────────────────────────────────────────────────

#[tokio::test]
async fn block_removes_follow_relationships() {
    let local_id = local_user_id();
    let s = setup(local_id).await;
    let data = s.config.to_request_data();

    let block = BlockActivity {
        id: activity_url("/block/1"),
        kind: BlockType,
        actor: ObjectId::from(remote_actor_url()),
        object: local_actor_url(),
    };

    use activitypub_federation::traits::Activity;
    block.receive(&data).await.unwrap();

    let removed_following = s.tracking.removed_following.lock().await;
    assert_eq!(
        removed_following.len(),
        1,
        "following should be removed on Block"
    );
    assert_eq!(removed_following[0].0, local_id);

    let removed_followers = s.tracking.removed_followers.lock().await;
    assert_eq!(
        removed_followers.len(),
        1,
        "follower should be removed on Block"
    );
    assert_eq!(removed_followers[0].0, local_id);
}

// ── Domain / actor blocking ─────────────────────────────────────────────────

#[tokio::test]
async fn activity_from_blocked_domain_is_skipped() {
    let local_id = local_user_id();
    let blocklist = MockBlocklistRepoBuilder::new()
        .on_is_domain_blocked(|domain| Ok(domain == "remote.example"))
        .build();
    let s = setup_with_blocklist(blocklist, local_id).await;
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
    let local_id = local_user_id();
    let expected_actor = REMOTE_ACTOR.to_string();
    let blocklist = MockBlocklistRepoBuilder::new()
        .on_is_actor_blocked(move |uid, url| Ok(uid == local_id && url == expected_actor))
        .build();
    let s = setup_with_blocklist(blocklist, local_id).await;
    let data = s.config.to_request_data();

    let follow = FollowActivity {
        id: activity_url("/follow/blocked"),
        kind: Default::default(),
        actor: ObjectId::from(remote_actor_url()),
        object: ObjectId::from(local_actor_url()),
    };

    use activitypub_federation::traits::Activity;
    follow.receive(&data).await.unwrap();

    let added = s.tracking.added_followers.lock().await;
    assert!(
        added.is_empty(),
        "blocked actor follow must be silently discarded"
    );
}

// ── Idempotency ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn duplicate_activity_id_is_skipped() {
    let local_id = local_user_id();
    let s = setup(local_id).await;
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
