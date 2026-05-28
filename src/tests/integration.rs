/// Integration tests exercising multi-component flows with in-memory trait stubs.
///
/// These tests don't spin up an HTTP server but they do exercise:
/// - `check_guards` idempotency (is_activity_processed → mark → duplicate rejected)
/// - `extract_and_dispatch_mentions` dispatches on_mention for local actors
/// - Multiple trait implementations wired through FederationData

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use tokio::sync::Mutex;
use url::Url;

use crate::content::ApObjectHandler;
use crate::data::FederationData;
use crate::repository::{
    BlockedDomain, FederationRepository, Follower, FollowerStatus, FollowingStatus, RemoteActor,
};
use crate::user::{ApActorType, ApProfileField, ApUser, ApUserRepository};

// ── In-memory FederationRepository ───────────────────────────────────────────

#[derive(Default)]
struct MemRepo {
    processed: Mutex<HashSet<String>>,
    blocked_domains: Mutex<HashSet<String>>,
}

#[async_trait]
impl FederationRepository for MemRepo {
    async fn is_activity_processed(&self, id: &str) -> anyhow::Result<bool> {
        Ok(self.processed.lock().await.contains(id))
    }
    async fn mark_activity_processed(&self, id: &str) -> anyhow::Result<()> {
        self.processed.lock().await.insert(id.to_string());
        Ok(())
    }
    async fn is_domain_blocked(&self, domain: &str) -> anyhow::Result<bool> {
        Ok(self.blocked_domains.lock().await.contains(domain))
    }
    // ── stubs ────────────────────────────────────────────────────────────────
    async fn add_follower(&self, _: uuid::Uuid, _: &str, _: FollowerStatus, _: &str) -> anyhow::Result<()> { Ok(()) }
    async fn get_follower_follow_activity_id(&self, _: uuid::Uuid, _: &str) -> anyhow::Result<Option<String>> { Ok(None) }
    async fn remove_follower(&self, _: uuid::Uuid, _: &str) -> anyhow::Result<()> { Ok(()) }
    async fn get_followers(&self, _: uuid::Uuid) -> anyhow::Result<Vec<Follower>> { Ok(vec![]) }
    async fn get_followers_page(&self, _: uuid::Uuid, _: u32, _: usize) -> anyhow::Result<Vec<Follower>> { Ok(vec![]) }
    async fn count_followers(&self, _: uuid::Uuid) -> anyhow::Result<usize> { Ok(0) }
    async fn get_following_page(&self, _: uuid::Uuid, _: u32, _: usize) -> anyhow::Result<Vec<RemoteActor>> { Ok(vec![]) }
    async fn update_follower_status(&self, _: uuid::Uuid, _: &str, _: FollowerStatus) -> anyhow::Result<()> { Ok(()) }
    async fn add_following(&self, _: uuid::Uuid, _: RemoteActor, _: &str) -> anyhow::Result<()> { Ok(()) }
    async fn get_follow_activity_id(&self, _: uuid::Uuid, _: &str) -> anyhow::Result<Option<String>> { Ok(None) }
    async fn remove_following(&self, _: uuid::Uuid, _: &str) -> anyhow::Result<()> { Ok(()) }
    async fn get_following(&self, _: uuid::Uuid) -> anyhow::Result<Vec<RemoteActor>> { Ok(vec![]) }
    async fn count_following(&self, _: uuid::Uuid) -> anyhow::Result<usize> { Ok(0) }
    async fn upsert_remote_actor(&self, _: RemoteActor) -> anyhow::Result<()> { Ok(()) }
    async fn get_remote_actor(&self, _: &str) -> anyhow::Result<Option<RemoteActor>> { Ok(None) }
    async fn get_local_actor_keypair(&self, _: uuid::Uuid) -> anyhow::Result<Option<(String, String)>> { Ok(None) }
    async fn save_local_actor_keypair(&self, _: uuid::Uuid, _: String, _: String) -> anyhow::Result<()> { Ok(()) }
    async fn get_pending_followers(&self, _: uuid::Uuid) -> anyhow::Result<Vec<RemoteActor>> { Ok(vec![]) }
    async fn update_following_status(&self, _: uuid::Uuid, _: &str, _: FollowingStatus) -> anyhow::Result<()> { Ok(()) }
    async fn get_following_outbox_url(&self, _: uuid::Uuid, _: &str) -> anyhow::Result<Option<String>> { Ok(None) }
    async fn add_announce(&self, _: &str, _: &str, _: &str, _: DateTime<Utc>) -> anyhow::Result<()> { Ok(()) }
    async fn count_announces(&self, _: &str) -> anyhow::Result<usize> { Ok(0) }
    async fn add_blocked_domain(&self, _: &str, _: Option<&str>) -> anyhow::Result<()> { Ok(()) }
    async fn remove_blocked_domain(&self, _: &str) -> anyhow::Result<()> { Ok(()) }
    async fn get_blocked_domains(&self) -> anyhow::Result<Vec<BlockedDomain>> { Ok(vec![]) }
    async fn add_blocked_actor(&self, _: uuid::Uuid, _: &str) -> anyhow::Result<()> { Ok(()) }
    async fn remove_blocked_actor(&self, _: uuid::Uuid, _: &str) -> anyhow::Result<()> { Ok(()) }
    async fn get_blocked_actors(&self, _: uuid::Uuid) -> anyhow::Result<Vec<String>> { Ok(vec![]) }
    async fn is_actor_blocked(&self, _: uuid::Uuid, _: &str) -> anyhow::Result<bool> { Ok(false) }
    async fn migrate_follower_actor(&self, _: &str, _: &str) -> anyhow::Result<Vec<uuid::Uuid>> { Ok(vec![]) }
    async fn get_accepted_follower_inboxes(&self, _: uuid::Uuid) -> anyhow::Result<Vec<String>> { Ok(vec![]) }
}

// ── In-memory ApUserRepository ────────────────────────────────────────────────

struct MemUserRepo {
    users: HashMap<uuid::Uuid, ApUser>,
}

impl MemUserRepo {
    fn with_user(id: uuid::Uuid, username: &str) -> Self {
        let mut users = HashMap::new();
        users.insert(id, ApUser {
            id,
            username: username.to_string(),
            display_name: None,
            bio: None,
            avatar_url: None,
            banner_url: None,
            also_known_as: None,
            profile_url: None,
            attachment: vec![],
            manually_approves_followers: true,
            discoverable: true,
            actor_type: ApActorType::Person,
            featured_url: None,
        });
        Self { users }
    }
}

#[async_trait]
impl ApUserRepository for MemUserRepo {
    async fn find_by_id(&self, id: uuid::Uuid) -> anyhow::Result<Option<ApUser>> {
        Ok(self.users.get(&id).cloned())
    }
    async fn find_by_username(&self, username: &str) -> anyhow::Result<Option<ApUser>> {
        Ok(self.users.values().find(|u| u.username == username).cloned())
    }
    async fn count_users(&self) -> anyhow::Result<usize> { Ok(self.users.len()) }
}

// ── In-memory ApObjectHandler ─────────────────────────────────────────────────

#[derive(Default)]
struct MemHandler {
    creates: Mutex<Vec<Url>>,
    mentions: Mutex<Vec<(Url, uuid::Uuid)>>,
}

#[async_trait]
impl ApObjectHandler for MemHandler {
    async fn get_local_objects_for_user(&self, _: uuid::Uuid) -> anyhow::Result<Vec<(Url, serde_json::Value)>> { Ok(vec![]) }
    async fn get_local_objects_page(&self, _: uuid::Uuid, _: Option<DateTime<Utc>>, _: usize) -> anyhow::Result<Vec<(Url, serde_json::Value, DateTime<Utc>)>> { Ok(vec![]) }
    async fn on_create(&self, ap_id: &Url, _: &Url, _: serde_json::Value) -> anyhow::Result<()> {
        self.creates.lock().await.push(ap_id.clone());
        Ok(())
    }
    async fn on_update(&self, _: &Url, _: &Url, _: serde_json::Value) -> anyhow::Result<()> { Ok(()) }
    async fn on_delete(&self, _: &Url, _: &Url) -> anyhow::Result<()> { Ok(()) }
    async fn on_actor_removed(&self, _: &Url) -> anyhow::Result<()> { Ok(()) }
    async fn on_like(&self, _: &Url, _: &Url) -> anyhow::Result<()> { Ok(()) }
    async fn on_announce_received(&self, _: &Url, _: &Url) -> anyhow::Result<()> { Ok(()) }
    async fn on_unlike(&self, _: &Url, _: &Url) -> anyhow::Result<()> { Ok(()) }
    async fn on_mention(&self, ap_id: &Url, user_id: uuid::Uuid, _: &Url) -> anyhow::Result<()> {
        self.mentions.lock().await.push((ap_id.clone(), user_id));
        Ok(())
    }
    async fn on_announce_of_remote(&self, _: &Url, _: &Url) -> anyhow::Result<()> { Ok(()) }
    async fn count_local_posts(&self) -> anyhow::Result<u64> { Ok(0) }
}

// ── Helper ────────────────────────────────────────────────────────────────────

fn make_data(
    repo: Arc<MemRepo>,
    user_repo: Arc<MemUserRepo>,
    handler: Arc<MemHandler>,
) -> FederationData {
    FederationData::new(
        repo,
        user_repo,
        handler,
        "https://example.com".to_string(),
        false,
        "test".to_string(),
        None,
    )
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn check_guards_idempotency() {
    use crate::activities::helpers::{already_processed, check_guards};
    use activitypub_federation::config::FederationConfig;

    let repo = Arc::new(MemRepo::default());
    let user_repo = Arc::new(MemUserRepo::with_user(uuid::Uuid::new_v4(), "alice"));
    let handler = Arc::new(MemHandler::default());
    let data_inner = make_data(repo, user_repo, handler);

    let config = FederationConfig::builder()
        .domain("example.com")
        .app_data(data_inner)
        .debug(true)
        .build()
        .await
        .unwrap();
    let data = config.to_request_data();

    let activity_id: Url = "https://remote.example/activities/abc123".parse().unwrap();
    let actor: Url = "https://remote.example/users/bob".parse().unwrap();

    // First call: not processed yet → should NOT skip
    let skip = check_guards(&activity_id, &actor, &data).await.unwrap();
    assert!(!skip, "first delivery should not be skipped");

    // Second call with same activity ID → should skip (duplicate)
    let skip = check_guards(&activity_id, &actor, &data).await.unwrap();
    assert!(skip, "duplicate delivery should be skipped");

    // Different activity ID → should not skip
    let other_id: Url = "https://remote.example/activities/xyz999".parse().unwrap();
    let skip = check_guards(&other_id, &actor, &data).await.unwrap();
    assert!(!skip, "different activity should not be skipped");
}

#[tokio::test]
async fn check_guards_blocks_domain() {
    use crate::activities::helpers::check_guards;
    use activitypub_federation::config::FederationConfig;

    let repo = Arc::new(MemRepo {
        blocked_domains: Mutex::new(["spam.example".to_string()].into()),
        ..Default::default()
    });
    let user_repo = Arc::new(MemUserRepo::with_user(uuid::Uuid::new_v4(), "alice"));
    let handler = Arc::new(MemHandler::default());
    let data_inner = make_data(repo, user_repo, handler);

    let config = FederationConfig::builder()
        .domain("example.com")
        .app_data(data_inner)
        .debug(true)
        .build()
        .await
        .unwrap();
    let data = config.to_request_data();

    let activity_id: Url = "https://spam.example/activities/1".parse().unwrap();
    let actor: Url = "https://spam.example/users/evil".parse().unwrap();

    // Blocked domain → should skip
    let skip = check_guards(&activity_id, &actor, &data).await.unwrap();
    assert!(skip, "activity from blocked domain should be skipped");
}

#[tokio::test]
async fn extract_and_dispatch_mentions_notifies_local_users() {
    use crate::activities::helpers::extract_and_dispatch_mentions;
    use activitypub_federation::config::FederationConfig;

    let local_user_id = uuid::Uuid::new_v4();
    let user_repo = Arc::new(MemUserRepo::with_user(local_user_id, "alice"));
    let handler = Arc::new(MemHandler::default());
    let repo = Arc::new(MemRepo::default());
    let data_inner = make_data(repo, user_repo.clone(), handler.clone());

    let config = FederationConfig::builder()
        .domain("example.com")
        .app_data(data_inner)
        .debug(true)
        .build()
        .await
        .unwrap();
    let data = config.to_request_data();

    let ap_id: Url = "https://remote.example/notes/1".parse().unwrap();
    let actor_url: Url = "https://remote.example/users/bob".parse().unwrap();

    // Object with a Mention tag pointing to local user URL
    let local_user_url = format!("https://example.com/users/{}", local_user_id);
    let object = serde_json::json!({
        "type": "Note",
        "id": ap_id.as_str(),
        "content": "Hello @alice",
        "tag": [
            {"type": "Mention", "href": local_user_url, "name": "@alice@example.com"}
        ]
    });

    extract_and_dispatch_mentions(&ap_id, &actor_url, &object, &data).await;

    let mentions = handler.mentions.lock().await;
    assert_eq!(mentions.len(), 1);
    assert_eq!(mentions[0].0, ap_id);
    assert_eq!(mentions[0].1, local_user_id);
}
