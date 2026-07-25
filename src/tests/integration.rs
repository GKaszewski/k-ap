// src/tests/integration.rs
/// Integration tests with mock builders.
use std::collections::HashSet;
use std::sync::Arc;

use tokio::sync::Mutex;
use url::Url;

use crate::data::FederationData;
use crate::testing::{
    MockActivityRepoBuilder, MockActorRepoBuilder, MockBlocklistRepoBuilder,
    MockContentReaderBuilder, MockFollowRepoBuilder, MockObjectHandlerBuilder, MockUserRepoBuilder,
};
use crate::user::{ApActorType, ApUser};

// ── Helpers ──────────────────────────────────────────────────────────────────

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

fn build_user_repo(id: uuid::Uuid, username: &str) -> Arc<crate::testing::MockUserRepo> {
    let user = make_user(id, username);
    let uname = username.to_string();
    let user2 = user.clone();
    MockUserRepoBuilder::new()
        .on_find_by_id(move |qid| {
            if qid == id {
                Ok(Some(user.clone()))
            } else {
                Ok(None)
            }
        })
        .on_find_by_username(move |name| {
            if name == uname {
                Ok(Some(user2.clone()))
            } else {
                Ok(None)
            }
        })
        .build()
}

// ── Helper ───────────────────────────────────────────────────────────────────

fn make_data(
    blocklist_repo: Option<Arc<crate::testing::MockBlocklistRepo>>,
    user_repo: Arc<crate::testing::MockUserRepo>,
    handler: Arc<crate::testing::MockObjectHandler>,
) -> FederationData {
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

    FederationData::new(
        activity_repo,
        MockFollowRepoBuilder::new().build(),
        MockActorRepoBuilder::new().build(),
        blocklist_repo.unwrap_or_else(|| MockBlocklistRepoBuilder::new().build()),
        user_repo,
        MockContentReaderBuilder::new().build(),
        handler,
        "https://example.com".to_string(),
        false,
        "test".to_string(),
        None,
        std::time::Duration::from_secs(24 * 60 * 60),
        Arc::new(crate::url_scheme::DefaultUrlScheme),
    )
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn check_guards_idempotency() {
    use crate::activities::helpers::check_guards;
    use activitypub_federation::config::FederationConfig;

    let data_inner = make_data(
        None,
        build_user_repo(uuid::Uuid::new_v4(), "alice"),
        MockObjectHandlerBuilder::new().build(),
    );
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

    let skip = check_guards(&activity_id, &actor, &data).await.unwrap();
    assert!(!skip, "first delivery should not be skipped");

    let skip = check_guards(&activity_id, &actor, &data).await.unwrap();
    assert!(skip, "duplicate delivery should be skipped");

    let other_id: Url = "https://remote.example/activities/xyz999".parse().unwrap();
    let skip = check_guards(&other_id, &actor, &data).await.unwrap();
    assert!(!skip, "different activity should not be skipped");
}

#[tokio::test]
async fn check_guards_blocks_domain() {
    use crate::activities::helpers::check_guards;
    use activitypub_federation::config::FederationConfig;

    let blocklist = MockBlocklistRepoBuilder::new()
        .on_is_domain_blocked(|domain| Ok(domain == "spam.example"))
        .build();
    let data_inner = make_data(
        Some(blocklist),
        build_user_repo(uuid::Uuid::new_v4(), "alice"),
        MockObjectHandlerBuilder::new().build(),
    );
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

    let skip = check_guards(&activity_id, &actor, &data).await.unwrap();
    assert!(skip, "activity from blocked domain should be skipped");
}

#[tokio::test]
async fn extract_and_dispatch_mentions_notifies_local_users() {
    use crate::activities::helpers::extract_and_dispatch_mentions;
    use activitypub_federation::config::FederationConfig;

    let local_user_id = uuid::Uuid::new_v4();
    let mentions: Arc<Mutex<Vec<(Url, uuid::Uuid)>>> = Arc::new(Mutex::new(vec![]));
    let m = mentions.clone();
    let handler = MockObjectHandlerBuilder::new()
        .on_on_mention(move |ap_id, user_id, _| {
            m.try_lock().unwrap().push((ap_id.clone(), user_id));
            Ok(())
        })
        .build();
    let data_inner = make_data(None, build_user_repo(local_user_id, "alice"), handler);
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
    let local_user_url = format!("https://example.com/users/{}", local_user_id);
    let object = serde_json::json!({
        "type": "Note",
        "id": ap_id.as_str(),
        "content": "Hello @alice",
        "tag": [{"type": "Mention", "href": local_user_url}]
    });

    extract_and_dispatch_mentions(&ap_id, &actor_url, &object, &data).await;

    let mentions = mentions.lock().await;
    assert_eq!(mentions.len(), 1);
    assert_eq!(mentions[0].0, ap_id);
    assert_eq!(mentions[0].1, local_user_id);
}
