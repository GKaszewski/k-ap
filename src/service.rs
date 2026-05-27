use std::sync::Arc;

use activitypub_federation::{
    activity_sending::SendActivityTask, fetch::object_id::ObjectId, protocol::context::WithContext,
    traits::Actor,
};
use axum::{Router, routing::get, routing::post};
use url::Url;

use crate::{
    activities::{
        AcceptActivity, CreateActivity, FollowActivity, RejectActivity, UndoActivity,
        UpdateActivity,
    },
    actors::{DbActor, get_local_actor},
    content::ApObjectHandler,
    data::FederationData,
    federation::ApFederationConfig,
    inbox::inbox_handler,
    nodeinfo::{nodeinfo_handler, nodeinfo_well_known_handler},
    outbox::outbox_handler,
    repository::{
        BlockedDomain, FederationRepository, FollowerStatus, FollowingStatus, RemoteActor,
    },
    urls::activity_url,
    user::ApUserRepository,
    webfinger::webfinger_handler,
};

const DELIVERY_MAX_ATTEMPTS: u32 = 3;
const DELIVERY_INITIAL_DELAY_SECS: u64 = 1;
const HTTP_FETCH_TIMEOUT_SECS: u64 = 30;
const BATCH_FETCH_SLEEP_MS: u64 = 100;

#[allow(dead_code)]
fn content_to_html(text: &str) -> String {
    let escaped = text
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;");
    let paragraphs: Vec<&str> = escaped.split('\n').filter(|s| !s.is_empty()).collect();
    if paragraphs.is_empty() {
        format!("<p>{}</p>", escaped)
    } else {
        paragraphs
            .iter()
            .map(|p| format!("<p>{}</p>", p))
            .collect::<Vec<_>>()
            .join("")
    }
}

fn collect_inboxes(followers: &[crate::repository::Follower]) -> Vec<Url> {
    let mut seen = std::collections::HashSet::new();
    let mut inboxes = Vec::new();
    for f in followers {
        let inbox_str = f
            .actor
            .shared_inbox_url
            .as_deref()
            .unwrap_or(&f.actor.inbox_url);
        if seen.insert(inbox_str.to_string())
            && let Ok(url) = Url::parse(inbox_str)
        {
            inboxes.push(url);
        }
    }
    inboxes
}

pub(crate) async fn send_with_retry(
    sends: Vec<SendActivityTask>,
    data: &activitypub_federation::config::Data<FederationData>,
) -> Vec<anyhow::Error> {
    let mut failures = vec![];
    for send in sends {
        let mut delay = std::time::Duration::from_secs(DELIVERY_INITIAL_DELAY_SECS);
        for attempt in 1..=DELIVERY_MAX_ATTEMPTS {
            match send.clone().sign_and_send(data).await {
                Ok(()) => break,
                Err(e) if attempt < DELIVERY_MAX_ATTEMPTS => {
                    tracing::warn!(attempt, error = %e, "delivery failed, retrying");
                    tokio::time::sleep(delay).await;
                    delay *= 2;
                }
                Err(e) => {
                    tracing::error!(attempt, error = %e, "delivery failed permanently");
                    failures.push(anyhow::anyhow!(e));
                }
            }
        }
    }
    failures
}

#[derive(Clone)]
pub struct ActivityPubService {
    federation_config: ApFederationConfig,
    base_url: String,
}

pub struct ActivityPubServiceBuilder {
    repo: Arc<dyn FederationRepository>,
    user_repo: Arc<dyn ApUserRepository>,
    object_handler: Arc<dyn ApObjectHandler>,
    base_url: String,
    allow_registration: bool,
    software_name: String,
    debug: bool,
    event_publisher: Option<Arc<dyn crate::data::EventPublisher>>,
}

impl ActivityPubServiceBuilder {
    pub fn allow_registration(mut self, v: bool) -> Self {
        self.allow_registration = v;
        self
    }
    pub fn software_name(mut self, v: impl Into<String>) -> Self {
        self.software_name = v.into();
        self
    }
    pub fn debug(mut self, v: bool) -> Self {
        self.debug = v;
        self
    }
    pub fn event_publisher(mut self, v: Arc<dyn crate::data::EventPublisher>) -> Self {
        self.event_publisher = Some(v);
        self
    }
    pub async fn build(self) -> anyhow::Result<ActivityPubService> {
        let data = FederationData::new(
            self.repo,
            self.user_repo,
            self.object_handler,
            self.base_url.clone(),
            self.allow_registration,
            self.software_name,
            self.event_publisher,
        );
        let federation_config = ApFederationConfig::new(data, self.debug).await?;
        Ok(ActivityPubService {
            federation_config,
            base_url: self.base_url,
        })
    }
}

impl ActivityPubService {
    pub fn builder(
        repo: Arc<dyn FederationRepository>,
        user_repo: Arc<dyn ApUserRepository>,
        object_handler: Arc<dyn ApObjectHandler>,
        base_url: impl Into<String>,
    ) -> ActivityPubServiceBuilder {
        ActivityPubServiceBuilder {
            repo,
            user_repo,
            object_handler,
            base_url: base_url.into(),
            allow_registration: false,
            software_name: String::new(),
            debug: false,
            event_publisher: None,
        }
    }

    pub fn federation_config(&self) -> &ApFederationConfig {
        &self.federation_config
    }

    pub fn request_data(&self) -> activitypub_federation::config::Data<FederationData> {
        self.federation_config.to_request_data()
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// Returns `(local_actor, deduplicated_inboxes)` for all accepted followers,
    /// excluding blocked actors and blocked domains.
    /// Returns `None` if there are no eligible followers.
    async fn accepted_follower_inboxes(
        &self,
        data: &activitypub_federation::config::Data<FederationData>,
        local_user_id: uuid::Uuid,
    ) -> anyhow::Result<Option<(crate::actors::DbActor, Vec<Url>)>> {
        let local_actor = get_local_actor(local_user_id, data)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;

        let followers = data.federation_repo.get_followers(local_user_id).await?;
        let blocked = data
            .federation_repo
            .get_blocked_actors(local_user_id)
            .await
            .unwrap_or_default();
        let blocked_set: std::collections::HashSet<String> = blocked.into_iter().collect();
        let blocked_domains = data
            .federation_repo
            .get_blocked_domains()
            .await
            .unwrap_or_default();
        let blocked_domain_set: std::collections::HashSet<String> =
            blocked_domains.into_iter().map(|d| d.domain).collect();

        let accepted: Vec<_> = followers
            .into_iter()
            .filter(|f| f.status == FollowerStatus::Accepted)
            .filter(|f| !blocked_set.contains(&f.actor.url))
            .filter(|f| {
                let domain = url::Url::parse(&f.actor.inbox_url)
                    .ok()
                    .and_then(|u| u.host_str().map(|s| s.to_string()))
                    .unwrap_or_default();
                !blocked_domain_set.contains(&domain)
            })
            .collect();

        if accepted.is_empty() {
            return Ok(None);
        }

        Ok(Some((local_actor, collect_inboxes(&accepted))))
    }

    /// Build an OrderedCollection or OrderedCollectionPage JSON for the local
    /// user's followers list.  Pass `page = None` for the root collection.
    pub async fn followers_collection_json(
        &self,
        user_id: uuid::Uuid,
        page: Option<u32>,
    ) -> anyhow::Result<String> {
        const AP_CONTEXT: &str = "https://www.w3.org/ns/activitystreams";
        const PAGE_SIZE: usize = 20;
        let data = self.federation_config.to_request_data();
        let collection_id = format!("{}/users/{}/followers", self.base_url, user_id);
        let total = data.federation_repo.count_followers(user_id).await?;
        let obj = if let Some(p) = page {
            let p = p.max(1);
            let offset = (p.saturating_sub(1) as usize) * PAGE_SIZE;
            let followers = data
                .federation_repo
                .get_followers_page(user_id, offset as u32, PAGE_SIZE)
                .await?;
            let has_next = offset + followers.len() < total;
            let items: Vec<String> = followers.into_iter().map(|f| f.actor.url).collect();
            let mut obj = serde_json::json!({
                "@context": AP_CONTEXT,
                "type": "OrderedCollectionPage",
                "id": format!("{}?page={}", collection_id, p),
                "partOf": collection_id,
                "totalItems": total,
                "orderedItems": items,
            });
            if has_next {
                obj["next"] =
                    serde_json::json!(format!("{}?page={}", collection_id, p + 1));
            }
            obj
        } else {
            serde_json::json!({
                "@context": AP_CONTEXT,
                "type": "OrderedCollection",
                "id": collection_id,
                "totalItems": total,
                "first": format!("{}?page=1", collection_id),
            })
        };
        Ok(serde_json::to_string(&obj)?)
    }

    /// Build an OrderedCollection or OrderedCollectionPage JSON for the local
    /// user's following list.  Pass `page = None` for the root collection.
    pub async fn following_collection_json(
        &self,
        user_id: uuid::Uuid,
        page: Option<u32>,
    ) -> anyhow::Result<String> {
        const AP_CONTEXT: &str = "https://www.w3.org/ns/activitystreams";
        const PAGE_SIZE: usize = 20;
        let data = self.federation_config.to_request_data();
        let collection_id = format!("{}/users/{}/following", self.base_url, user_id);
        let total = data.federation_repo.count_following(user_id).await?;
        let obj = if let Some(p) = page {
            let p = p.max(1);
            let offset = (p.saturating_sub(1) as usize) * PAGE_SIZE;
            let following = data
                .federation_repo
                .get_following_page(user_id, offset as u32, PAGE_SIZE)
                .await?;
            let has_next = offset + following.len() < total;
            let items: Vec<String> = following.into_iter().map(|a| a.url).collect();
            let mut obj = serde_json::json!({
                "@context": AP_CONTEXT,
                "type": "OrderedCollectionPage",
                "id": format!("{}?page={}", collection_id, p),
                "partOf": collection_id,
                "totalItems": total,
                "orderedItems": items,
            });
            if has_next {
                obj["next"] =
                    serde_json::json!(format!("{}?page={}", collection_id, p + 1));
            }
            obj
        } else {
            serde_json::json!({
                "@context": AP_CONTEXT,
                "type": "OrderedCollection",
                "id": collection_id,
                "totalItems": total,
                "first": format!("{}?page=1", collection_id),
            })
        };
        Ok(serde_json::to_string(&obj)?)
    }

    pub async fn actor_json(&self, user_id_str: &str) -> anyhow::Result<String> {
        use activitypub_federation::traits::Object;
        let uuid = uuid::Uuid::parse_str(user_id_str)?;
        let data = self.federation_config.to_request_data();
        let actor = get_local_actor(uuid, &data)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let person = actor
            .into_json(&data)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        Ok(serde_json::to_string(&WithContext::new_default(person))?)
    }

    /// Resolve a `@user@domain` handle to actor data using a signed HTTP request.
    /// Unlike a plain unauthenticated fetch, this works with instances (e.g. Threads)
    /// that require HTTP signatures before returning full actor JSON.
    pub async fn lookup_actor_by_handle(
        &self,
        handle: &str,
    ) -> anyhow::Result<crate::user::LookedUpActor> {
        let data = self.federation_config.to_request_data();
        let actor = Self::webfinger_https(handle, &data).await?;
        let domain = actor.ap_id.host_str().unwrap_or("").to_string();
        let handle = format!("{}@{}", actor.username, domain);
        Ok(crate::user::LookedUpActor {
            handle,
            display_name: actor.display_name,
            bio: actor.bio,
            avatar_url: actor.avatar_url,
            banner_url: actor.banner_url,
            ap_url: actor.ap_id,
            outbox_url: Some(actor.outbox_url),
            followers_url: Some(actor.followers_url),
            following_url: Some(actor.following_url),
            also_known_as: actor.also_known_as,
            profile_url: actor.profile_url,
            attachment: actor.attachment,
        })
    }

    /// Returns the ActivityPub router compatible with any outer state `S`.
    /// Handlers only use `Data<FederationData>` injected by the middleware layer,
    /// so the router is independent of the application state type.
    pub fn router<S>(&self) -> Router<S>
    where
        S: Clone + Send + Sync + 'static,
    {
        Router::new()
            .route("/.well-known/nodeinfo", get(nodeinfo_well_known_handler))
            .route("/nodeinfo/2.0", get(nodeinfo_handler))
            .route("/.well-known/webfinger", get(webfinger_handler))
            .route("/inbox", post(inbox_handler))
            .route("/users/{id}/inbox", post(inbox_handler))
            .route("/users/{id}/outbox", get(outbox_handler))
            .layer(self.federation_config.middleware())
    }

    /// Fan out an Announce activity to all accepted followers.
    pub async fn broadcast_announce_to_followers(
        &self,
        local_user_id: uuid::Uuid,
        object_ap_id: url::Url,
    ) -> anyhow::Result<()> {
        // Deterministic ID so Undo(Announce) can reference this same activity.
        let announce_id = url::Url::parse(&format!(
            "{}/activities/announce/{}",
            self.base_url,
            uuid::Uuid::new_v5(
                &uuid::Uuid::NAMESPACE_URL,
                format!("{}/{}", local_user_id, object_ap_id).as_bytes(),
            )
        ))
        .map_err(|e| anyhow::anyhow!("{e}"))?;

        let data = self.federation_config.to_request_data();
        let Some((local_actor, inboxes)) =
            self.accepted_follower_inboxes(&data, local_user_id).await?
        else {
            return Ok(());
        };

        let announce = crate::activities::AnnounceActivity {
            id: announce_id,
            kind: Default::default(),
            actor: activitypub_federation::fetch::object_id::ObjectId::from(
                local_actor.ap_id.clone(),
            ),
            object: object_ap_id,
            published: Some(chrono::Utc::now()),
            to: vec![crate::urls::AS_PUBLIC.to_string()],
            cc: vec![local_actor.followers_url.to_string()],
        };

        let sends = activitypub_federation::activity_sending::SendActivityTask::prepare(
            &activitypub_federation::protocol::context::WithContext::new_default(announce),
            &local_actor,
            inboxes,
            &data,
        )
        .await?;
        let failures = send_with_retry(sends, &data).await;
        if !failures.is_empty() {
            tracing::warn!(count = failures.len(), "some Announce deliveries failed");
        }
        Ok(())
    }

    /// Fan out an Undo(Announce) activity to all accepted followers.
    pub async fn broadcast_undo_announce_to_followers(
        &self,
        local_user_id: uuid::Uuid,
        object_ap_id: url::Url,
    ) -> anyhow::Result<()> {
        // Reconstruct the same deterministic announce ID used when the boost was sent.
        let announce_id = url::Url::parse(&format!(
            "{}/activities/announce/{}",
            self.base_url,
            uuid::Uuid::new_v5(
                &uuid::Uuid::NAMESPACE_URL,
                format!("{}/{}", local_user_id, object_ap_id).as_bytes(),
            )
        ))
        .map_err(|e| anyhow::anyhow!("{e}"))?;

        let undo_id =
            crate::urls::activity_url(&self.base_url).map_err(|e| anyhow::anyhow!("{e}"))?;

        let data = self.federation_config.to_request_data();
        let Some((local_actor, inboxes)) =
            self.accepted_follower_inboxes(&data, local_user_id).await?
        else {
            return Ok(());
        };

        let undo = crate::activities::UndoActivity {
            id: undo_id,
            kind: Default::default(),
            actor: activitypub_federation::fetch::object_id::ObjectId::from(
                local_actor.ap_id.clone(),
            ),
            object: serde_json::json!({
                "type": "Announce",
                "id": announce_id.to_string(),
                "actor": local_actor.ap_id.to_string(),
                "object": object_ap_id.to_string(),
            }),
        };

        let sends = activitypub_federation::activity_sending::SendActivityTask::prepare(
            &activitypub_federation::protocol::context::WithContext::new_default(undo),
            &local_actor,
            inboxes,
            &data,
        )
        .await?;
        let failures = send_with_retry(sends, &data).await;
        if !failures.is_empty() {
            tracing::warn!(
                count = failures.len(),
                "some Undo(Announce) deliveries failed"
            );
        }
        Ok(())
    }

    /// Send a Like activity to a single inbox.
    pub async fn broadcast_like_to_inbox(
        &self,
        liker_user_id: uuid::Uuid,
        object_ap_id: url::Url,
        author_inbox_url: url::Url,
    ) -> anyhow::Result<()> {
        let data = self.federation_config.to_request_data();
        let local_actor = get_local_actor(liker_user_id, &data)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;

        // Deterministic ID so Undo(Like) can reference the same activity.
        let like_id = url::Url::parse(&format!(
            "{}/activities/like/{}",
            self.base_url,
            uuid::Uuid::new_v5(
                &uuid::Uuid::NAMESPACE_URL,
                format!("{}/{}", liker_user_id, object_ap_id).as_bytes(),
            )
        ))?;

        let like = crate::activities::LikeActivity {
            id: like_id,
            kind: Default::default(),
            actor: ObjectId::from(local_actor.ap_id.clone()),
            object: object_ap_id,
        };

        let sends = SendActivityTask::prepare(
            &WithContext::new_default(like),
            &local_actor,
            vec![author_inbox_url],
            &data,
        )
        .await?;
        let failures = send_with_retry(sends, &data).await;
        if !failures.is_empty() {
            tracing::warn!(
                count = failures.len(),
                "some Like deliveries failed permanently"
            );
        }
        Ok(())
    }

    /// Send an Undo(Like) activity to a single inbox.
    pub async fn broadcast_undo_like_to_inbox(
        &self,
        liker_user_id: uuid::Uuid,
        object_ap_id: url::Url,
        author_inbox_url: url::Url,
    ) -> anyhow::Result<()> {
        let data = self.federation_config.to_request_data();
        let local_actor = get_local_actor(liker_user_id, &data)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;

        // Reconstruct the same deterministic like ID.
        let like_id = url::Url::parse(&format!(
            "{}/activities/like/{}",
            self.base_url,
            uuid::Uuid::new_v5(
                &uuid::Uuid::NAMESPACE_URL,
                format!("{}/{}", liker_user_id, object_ap_id).as_bytes(),
            )
        ))?;

        let undo_id = activity_url(&self.base_url).map_err(|e| anyhow::anyhow!("{e}"))?;

        let undo = crate::activities::UndoActivity {
            id: undo_id,
            kind: Default::default(),
            actor: ObjectId::from(local_actor.ap_id.clone()),
            object: serde_json::json!({
                "type": "Like",
                "id": like_id.to_string(),
                "actor": local_actor.ap_id.to_string(),
                "object": object_ap_id.to_string(),
            }),
        };

        let sends = SendActivityTask::prepare(
            &WithContext::new_default(undo),
            &local_actor,
            vec![author_inbox_url],
            &data,
        )
        .await?;
        let failures = send_with_retry(sends, &data).await;
        if !failures.is_empty() {
            tracing::warn!(
                count = failures.len(),
                "some Undo(Like) deliveries failed permanently"
            );
        }
        Ok(())
    }

    /// Resolve a `@user@domain` handle to a `DbActor` over HTTPS directly.
    /// The library's `webfinger_resolve_actor` tries HTTP first in debug mode, which breaks
    /// on servers that don't redirect HTTP → HTTPS.
    async fn webfinger_https(
        handle: &str,
        data: &activitypub_federation::config::Data<FederationData>,
    ) -> anyhow::Result<DbActor> {
        let normalized = handle.trim_start_matches('@');
        let at = normalized
            .rfind('@')
            .ok_or_else(|| anyhow::anyhow!("handle must be user@domain"))?;
        let (user, domain_str) = (&normalized[..at], &normalized[at + 1..]);
        let wf_url = format!(
            "https://{}/.well-known/webfinger?resource=acct:{}@{}",
            domain_str, user, domain_str
        );
        let wf: serde_json::Value = reqwest::Client::new()
            .get(&wf_url)
            .header("Accept", "application/jrd+json, application/json")
            .send()
            .await?
            .json()
            .await?;
        let self_href = wf["links"]
            .as_array()
            .and_then(|links| {
                links.iter().find(|l| {
                    l["rel"].as_str() == Some("self")
                        && l["type"].as_str() == Some("application/activity+json")
                })
            })
            .and_then(|l| l["href"].as_str())
            .ok_or_else(|| anyhow::anyhow!("no self link in WebFinger response"))?
            .to_owned();
        let self_url = url::Url::parse(&self_href)?;
        let actor: DbActor = ObjectId::from(self_url)
            .dereference(data)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        Ok(actor)
    }

    pub async fn follow(&self, local_user_id: uuid::Uuid, handle: &str) -> anyhow::Result<()> {
        let data = self.federation_config.to_request_data();

        let normalized = handle.trim_start_matches('@');
        let parts: Vec<&str> = normalized.splitn(2, '@').collect();
        if parts.len() == 2 && parts[1] == data.domain {
            return self.follow_local(local_user_id, parts[0], &data).await;
        }

        let remote_actor: DbActor = Self::webfinger_https(handle, &data).await?;

        let local_actor = get_local_actor(local_user_id, &data)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;

        let follow_id = activity_url(&self.base_url).map_err(|e| anyhow::anyhow!("{e}"))?;
        let follow_id_str = follow_id.to_string();
        let follow = FollowActivity {
            id: follow_id,
            kind: Default::default(),
            actor: ObjectId::from(local_actor.ap_id.clone()),
            object: ObjectId::from(remote_actor.ap_id.clone()),
        };
        let follow_with_ctx = WithContext::new_default(follow);

        let sends = SendActivityTask::prepare(
            &follow_with_ctx,
            &local_actor,
            vec![remote_actor.inbox()],
            &data,
        )
        .await?;
        let failures = send_with_retry(sends, &data).await;
        if !failures.is_empty() {
            tracing::warn!(
                count = failures.len(),
                "some activity deliveries failed permanently"
            );
        }

        let domain = remote_actor.ap_id.host_str().unwrap_or("");
        let full_handle = format!("{}@{}", remote_actor.username, domain);
        let remote = RemoteActor {
            url: remote_actor.ap_id.to_string(),
            handle: full_handle,
            inbox_url: remote_actor.inbox_url.to_string(),
            shared_inbox_url: remote_actor
                .shared_inbox_url
                .as_ref()
                .map(|u| u.to_string()),
            display_name: Some(remote_actor.username.clone()),
            avatar_url: remote_actor.avatar_url.as_ref().map(|u| u.to_string()),
            outbox_url: Some(remote_actor.outbox_url.to_string()),
        };
        data.federation_repo
            .add_following(local_user_id, remote, &follow_id_str)
            .await?;

        Ok(())
    }

    pub async fn unfollow(
        &self,
        local_user_id: uuid::Uuid,
        actor_url_str: &str,
    ) -> anyhow::Result<()> {
        let data = self.federation_config.to_request_data();

        if actor_url_str.starts_with(&self.base_url) {
            return self
                .unfollow_local(local_user_id, actor_url_str, &data)
                .await;
        }

        let remote = data
            .federation_repo
            .get_remote_actor(actor_url_str)
            .await?
            .ok_or_else(|| anyhow::anyhow!("remote actor not found: {}", actor_url_str))?;

        let local_actor = get_local_actor(local_user_id, &data)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;

        let remote_ap_id = Url::parse(actor_url_str)?;
        let inbox = Url::parse(&remote.inbox_url)?;

        let follow_activity_id_str = data
            .federation_repo
            .get_follow_activity_id(local_user_id, actor_url_str)
            .await?;
        let follow_id = match follow_activity_id_str {
            Some(id) => Url::parse(&id)?,
            None => activity_url(&self.base_url).map_err(|e| anyhow::anyhow!("{e}"))?,
        };
        let follow = FollowActivity {
            id: follow_id,
            kind: Default::default(),
            actor: ObjectId::from(local_actor.ap_id.clone()),
            object: ObjectId::from(remote_ap_id),
        };

        let undo_id = activity_url(&self.base_url).map_err(|e| anyhow::anyhow!("{e}"))?;
        let undo = UndoActivity {
            id: undo_id,
            kind: Default::default(),
            actor: ObjectId::from(local_actor.ap_id.clone()),
            object: serde_json::to_value(&follow).map_err(|e| anyhow::anyhow!("{e}"))?,
        };

        let sends = SendActivityTask::prepare(
            &WithContext::new_default(undo),
            &local_actor,
            vec![inbox],
            &data,
        )
        .await?;
        let failures = send_with_retry(sends, &data).await;
        if !failures.is_empty() {
            tracing::warn!(
                count = failures.len(),
                "some activity deliveries failed permanently"
            );
        }

        data.federation_repo
            .remove_following(local_user_id, actor_url_str)
            .await?;

        data.object_handler
            .on_actor_removed(&Url::parse(actor_url_str)?)
            .await?;

        Ok(())
    }

    pub async fn accept_follower(
        &self,
        local_user_id: uuid::Uuid,
        remote_actor_url: &str,
    ) -> anyhow::Result<()> {
        let data = self.federation_config.to_request_data();
        let local_actor = get_local_actor(local_user_id, &data)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;

        let remote_actor = data
            .federation_repo
            .get_remote_actor(remote_actor_url)
            .await?
            .ok_or_else(|| anyhow::anyhow!("remote actor not found"))?;

        let follow_id_str = data
            .federation_repo
            .get_follower_follow_activity_id(local_user_id, remote_actor_url)
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!("follow activity id not found for {}", remote_actor_url)
            })?;
        let follow_id = Url::parse(&follow_id_str)?;
        let follow = FollowActivity {
            id: follow_id,
            kind: Default::default(),
            actor: ObjectId::from(Url::parse(remote_actor_url)?),
            object: ObjectId::from(local_actor.ap_id.clone()),
        };
        let accept = AcceptActivity {
            id: activity_url(&self.base_url).map_err(|e| anyhow::anyhow!("{e}"))?,
            kind: Default::default(),
            actor: ObjectId::from(local_actor.ap_id.clone()),
            object: follow,
        };

        data.federation_repo
            .update_follower_status(local_user_id, remote_actor_url, FollowerStatus::Accepted)
            .await?;

        let inbox = Url::parse(&remote_actor.inbox_url)?;
        let sends = SendActivityTask::prepare(
            &WithContext::new_default(accept),
            &local_actor,
            vec![inbox.clone()],
            &data,
        )
        .await?;
        let failures = send_with_retry(sends, &data).await;
        if !failures.is_empty() {
            tracing::warn!(
                "failed to deliver Accept activity, but follower is marked accepted locally"
            );
        }

        let target_inbox = remote_actor
            .shared_inbox_url
            .clone()
            .unwrap_or_else(|| remote_actor.inbox_url.clone());
        self.spawn_backfill(local_user_id, target_inbox);

        Ok(())
    }

    pub async fn reject_follower(
        &self,
        local_user_id: uuid::Uuid,
        remote_actor_url: &str,
    ) -> anyhow::Result<()> {
        let data = self.federation_config.to_request_data();
        let local_actor = get_local_actor(local_user_id, &data)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;

        let remote_actor = data
            .federation_repo
            .get_remote_actor(remote_actor_url)
            .await?
            .ok_or_else(|| anyhow::anyhow!("remote actor not found"))?;

        let follow_id = activity_url(&self.base_url).map_err(|e| anyhow::anyhow!("{e}"))?;
        let follow = FollowActivity {
            id: follow_id,
            kind: Default::default(),
            actor: ObjectId::from(Url::parse(remote_actor_url)?),
            object: ObjectId::from(local_actor.ap_id.clone()),
        };
        let reject = RejectActivity {
            id: activity_url(&self.base_url).map_err(|e| anyhow::anyhow!("{e}"))?,
            kind: Default::default(),
            actor: ObjectId::from(local_actor.ap_id.clone()),
            object: follow,
        };

        let inbox = Url::parse(&remote_actor.inbox_url)?;
        let sends = SendActivityTask::prepare(
            &WithContext::new_default(reject),
            &local_actor,
            vec![inbox],
            &data,
        )
        .await?;
        let failures = send_with_retry(sends, &data).await;
        if !failures.is_empty() {
            tracing::warn!(
                count = failures.len(),
                "some activity deliveries failed permanently"
            );
        }

        data.federation_repo
            .remove_follower(local_user_id, remote_actor_url)
            .await?;

        Ok(())
    }

    pub async fn get_pending_followers(
        &self,
        local_user_id: uuid::Uuid,
    ) -> anyhow::Result<Vec<RemoteActor>> {
        let data = self.federation_config.to_request_data();
        data.federation_repo
            .get_pending_followers(local_user_id)
            .await
    }

    pub async fn get_accepted_followers(
        &self,
        local_user_id: uuid::Uuid,
    ) -> anyhow::Result<Vec<RemoteActor>> {
        let data = self.federation_config.to_request_data();
        let followers = data.federation_repo.get_followers(local_user_id).await?;
        Ok(followers
            .into_iter()
            .filter(|f| f.status == FollowerStatus::Accepted)
            .map(|f| f.actor)
            .collect())
    }

    pub async fn count_accepted_followers(
        &self,
        local_user_id: uuid::Uuid,
    ) -> anyhow::Result<usize> {
        let data = self.federation_config.to_request_data();
        let followers = data.federation_repo.get_followers(local_user_id).await?;
        Ok(followers
            .into_iter()
            .filter(|f| f.status == FollowerStatus::Accepted)
            .count())
    }

    pub async fn get_following(
        &self,
        local_user_id: uuid::Uuid,
    ) -> anyhow::Result<Vec<RemoteActor>> {
        let data = self.federation_config.to_request_data();
        data.federation_repo.get_following(local_user_id).await
    }

    pub async fn count_following(&self, local_user_id: uuid::Uuid) -> anyhow::Result<usize> {
        let data = self.federation_config.to_request_data();
        data.federation_repo.count_following(local_user_id).await
    }

    pub async fn remove_follower(
        &self,
        local_user_id: uuid::Uuid,
        actor_url: &str,
    ) -> anyhow::Result<()> {
        let data = self.federation_config.to_request_data();
        data.federation_repo
            .remove_follower(local_user_id, actor_url)
            .await
    }

    /// Broadcast a Delete activity to all accepted followers for a removed review.
    pub async fn broadcast_delete_to_followers(
        &self,
        local_user_id: uuid::Uuid,
        ap_id: Url,
    ) -> anyhow::Result<()> {
        let data = self.federation_config.to_request_data();
        let Some((local_actor, inboxes)) =
            self.accepted_follower_inboxes(&data, local_user_id).await?
        else {
            return Ok(());
        };

        let delete_id =
            crate::urls::activity_url(&self.base_url).map_err(|e| anyhow::anyhow!("{e}"))?;
        let delete = crate::activities::DeleteActivity {
            id: delete_id,
            kind: Default::default(),
            actor: ObjectId::from(local_actor.ap_id.clone()),
            object: serde_json::json!(ap_id.to_string()),
            to: vec![crate::urls::AS_PUBLIC.to_string()],
            cc: vec![local_actor.followers_url.to_string()],
        };
        let delete_with_ctx = WithContext::new_default(delete);
        let sends =
            SendActivityTask::prepare(&delete_with_ctx, &local_actor, inboxes, &data).await?;
        let failures = send_with_retry(sends, &data).await;
        if !failures.is_empty() {
            tracing::warn!(
                count = failures.len(),
                "some delete activity deliveries failed"
            );
        }
        Ok(())
    }

    /// Broadcast an Add(WatchlistObject) activity to all accepted followers.
    pub async fn broadcast_add_to_followers(
        &self,
        local_user_id: uuid::Uuid,
        ap_id: Url,
        object: serde_json::Value,
    ) -> anyhow::Result<()> {
        let data = self.federation_config.to_request_data();
        let Some((local_actor, inboxes)) =
            self.accepted_follower_inboxes(&data, local_user_id).await?
        else {
            return Ok(());
        };

        let add = crate::activities::AddActivity {
            id: ap_id,
            kind: Default::default(),
            actor: ObjectId::from(local_actor.ap_id.clone()),
            object,
            to: vec![crate::urls::AS_PUBLIC.to_string()],
            cc: vec![local_actor.followers_url.to_string()],
        };
        let add_with_ctx = WithContext::new_default(add);
        let sends = SendActivityTask::prepare(&add_with_ctx, &local_actor, inboxes, &data).await?;
        let failures = send_with_retry(sends, &data).await;
        if !failures.is_empty() {
            tracing::warn!(count = failures.len(), "some Add deliveries failed");
        }
        Ok(())
    }

    /// Broadcast an Undo(Add) activity to all accepted followers.
    pub async fn broadcast_undo_add_to_followers(
        &self,
        local_user_id: uuid::Uuid,
        watchlist_entry_ap_id: Url,
    ) -> anyhow::Result<()> {
        let data = self.federation_config.to_request_data();
        let Some((local_actor, inboxes)) =
            self.accepted_follower_inboxes(&data, local_user_id).await?
        else {
            return Ok(());
        };

        let undo_id =
            crate::urls::activity_url(&self.base_url).map_err(|e| anyhow::anyhow!("{e}"))?;
        let undo = crate::activities::UndoActivity {
            id: undo_id,
            kind: Default::default(),
            actor: ObjectId::from(local_actor.ap_id.clone()),
            object: serde_json::json!({
                "type": "Add",
                "id": watchlist_entry_ap_id.as_str(),
                "object": { "id": watchlist_entry_ap_id.as_str() }
            }),
        };
        let undo_with_ctx = WithContext::new_default(undo);
        let sends = SendActivityTask::prepare(&undo_with_ctx, &local_actor, inboxes, &data).await?;
        let failures = send_with_retry(sends, &data).await;
        if !failures.is_empty() {
            tracing::warn!(count = failures.len(), "some Undo(Add) deliveries failed");
        }
        Ok(())
    }

    /// Fan out a Create(Note) activity to all accepted followers.
    /// `note` is the fully-formed Note JSON (including id, type, content, etc.).
    /// The activity ID is derived deterministically from the note's `id` field.
    pub async fn broadcast_create_note(
        &self,
        local_user_id: uuid::Uuid,
        note: serde_json::Value,
    ) -> anyhow::Result<()> {
        let data = self.federation_config.to_request_data();
        let Some((local_actor, inboxes)) =
            self.accepted_follower_inboxes(&data, local_user_id).await?
        else {
            return Ok(());
        };

        let note_id_str = note["id"].as_str().unwrap_or("");
        let create_id = Url::parse(&format!(
            "{}/activities/create/{}",
            self.base_url,
            uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_URL, note_id_str.as_bytes())
        ))
        .map_err(|e| anyhow::anyhow!("{e}"))?;

        let create = crate::activities::CreateActivity {
            id: create_id,
            kind: Default::default(),
            actor: ObjectId::from(local_actor.ap_id.clone()),
            object: note,
            to: vec![crate::urls::AS_PUBLIC.to_string()],
            cc: vec![local_actor.followers_url.to_string()],
            bto: vec![],
            bcc: vec![],
        };
        let sends = SendActivityTask::prepare(
            &WithContext::new_default(create),
            &local_actor,
            inboxes,
            &data,
        )
        .await?;
        let failures = send_with_retry(sends, &data).await;
        if !failures.is_empty() {
            tracing::warn!(count = failures.len(), "some Create(Note) deliveries failed");
        }
        Ok(())
    }

    /// Fan out an Update(Note) activity to all accepted followers.
    /// `note` is the fully-formed Note JSON.
    pub async fn broadcast_update_note(
        &self,
        local_user_id: uuid::Uuid,
        note: serde_json::Value,
    ) -> anyhow::Result<()> {
        let data = self.federation_config.to_request_data();
        let Some((local_actor, inboxes)) =
            self.accepted_follower_inboxes(&data, local_user_id).await?
        else {
            return Ok(());
        };

        let update_id = crate::urls::activity_url(&self.base_url)
            .map_err(|e| anyhow::anyhow!("{e}"))?;

        let update = crate::activities::UpdateActivity {
            id: update_id,
            kind: Default::default(),
            actor: ObjectId::from(local_actor.ap_id.clone()),
            object: note,
            to: vec![crate::urls::AS_PUBLIC.to_string()],
            cc: vec![local_actor.followers_url.to_string()],
        };
        let sends = SendActivityTask::prepare(
            &WithContext::new_default(update),
            &local_actor,
            inboxes,
            &data,
        )
        .await?;
        let failures = send_with_retry(sends, &data).await;
        if !failures.is_empty() {
            tracing::warn!(count = failures.len(), "some Update(Note) deliveries failed");
        }
        Ok(())
    }

    pub async fn broadcast_actor_update(&self, user_id: uuid::Uuid) -> anyhow::Result<()> {
        use activitypub_federation::traits::Object;

        let data = self.federation_config.to_request_data();
        let local_actor = get_local_actor(user_id, &data)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;

        let person = local_actor
            .clone()
            .into_json(&data)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        // Wrap with @context so Mastodon's JSON-LD processor can resolve field names.
        let person_json = serde_json::to_value(WithContext::new_default(person))?;

        let update_id = Url::parse(&format!(
            "{}/activities/update/{}",
            self.base_url,
            uuid::Uuid::new_v4()
        ))?;

        let update = UpdateActivity {
            id: update_id,
            kind: Default::default(),
            actor: ObjectId::from(local_actor.ap_id.clone()),
            object: person_json,
            to: vec![crate::urls::AS_PUBLIC.to_string()],
            cc: vec![local_actor.followers_url.to_string()],
        };

        let followers = data.federation_repo.get_followers(user_id).await?;
        let accepted: Vec<_> = followers
            .into_iter()
            .filter(|f| f.status == FollowerStatus::Accepted)
            .collect();

        if accepted.is_empty() {
            tracing::info!(user_id = %user_id, "no accepted followers, skipping actor update broadcast");
            return Ok(());
        }

        let inboxes = collect_inboxes(&accepted);
        tracing::info!(
            user_id = %user_id,
            follower_count = accepted.len(),
            inbox_count = inboxes.len(),
            inboxes = ?inboxes,
            "broadcasting actor update"
        );

        let sends = SendActivityTask::prepare(
            &WithContext::new_default(update),
            &local_actor,
            inboxes,
            &data,
        )
        .await?;

        let failures = send_with_retry(sends, &data).await;
        if !failures.is_empty() {
            return Err(anyhow::anyhow!(
                "actor update delivery failed for {} inbox(es): {}",
                failures.len(),
                failures
                    .iter()
                    .map(|e| e.to_string())
                    .collect::<Vec<_>>()
                    .join("; ")
            ));
        }
        tracing::info!(user_id = %user_id, "actor update broadcast complete");
        Ok(())
    }

    pub async fn block_actor(
        &self,
        local_user_id: uuid::Uuid,
        actor_url: &str,
    ) -> anyhow::Result<()> {
        let data = self.federation_config.to_request_data();

        data.federation_repo
            .add_blocked_actor(local_user_id, actor_url)
            .await?;
        let _ = data
            .federation_repo
            .remove_follower(local_user_id, actor_url)
            .await;
        let _ = data
            .federation_repo
            .remove_following(local_user_id, actor_url)
            .await;

        let local_actor = get_local_actor(local_user_id, &data)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;

        if let Ok(Some(remote_actor)) = data.federation_repo.get_remote_actor(actor_url).await {
            let block_id =
                crate::urls::activity_url(&self.base_url).map_err(|e| anyhow::anyhow!("{e}"))?;
            let block = crate::activities::BlockActivity {
                id: block_id,
                kind: Default::default(),
                actor: ObjectId::from(local_actor.ap_id.clone()),
                object: Url::parse(actor_url)?,
            };
            let inbox = Url::parse(&remote_actor.inbox_url)?;
            let sends = SendActivityTask::prepare(
                &WithContext::new_default(block),
                &local_actor,
                vec![inbox],
                &data,
            )
            .await?;
            let failures = send_with_retry(sends, &data).await;
            if !failures.is_empty() {
                tracing::warn!(actor = %actor_url, "failed to deliver Block activity");
            }
        }

        Ok(())
    }

    pub async fn unblock_actor(
        &self,
        local_user_id: uuid::Uuid,
        actor_url: &str,
    ) -> anyhow::Result<()> {
        let data = self.federation_config.to_request_data();
        data.federation_repo
            .remove_blocked_actor(local_user_id, actor_url)
            .await
    }

    pub async fn get_blocked_actors(
        &self,
        local_user_id: uuid::Uuid,
    ) -> anyhow::Result<Vec<RemoteActor>> {
        let data = self.federation_config.to_request_data();
        let actor_urls = data
            .federation_repo
            .get_blocked_actors(local_user_id)
            .await?;
        let mut actors = Vec::new();
        for url in actor_urls {
            let actor = match data.federation_repo.get_remote_actor(&url).await {
                Ok(Some(a)) => a,
                _ => RemoteActor {
                    url: url.clone(),
                    handle: url.clone(),
                    inbox_url: url.clone(),
                    shared_inbox_url: None,
                    display_name: None,
                    avatar_url: None,
                    outbox_url: None,
                },
            };
            actors.push(actor);
        }
        Ok(actors)
    }

    pub async fn add_blocked_domain(
        &self,
        domain: &str,
        reason: Option<&str>,
    ) -> anyhow::Result<()> {
        let data = self.federation_config.to_request_data();
        data.federation_repo
            .add_blocked_domain(domain, reason)
            .await
    }

    pub async fn remove_blocked_domain(&self, domain: &str) -> anyhow::Result<()> {
        let data = self.federation_config.to_request_data();
        data.federation_repo.remove_blocked_domain(domain).await
    }

    pub async fn get_blocked_domains(&self) -> anyhow::Result<Vec<BlockedDomain>> {
        let data = self.federation_config.to_request_data();
        data.federation_repo.get_blocked_domains().await
    }

    async fn follow_local(
        &self,
        local_user_id: uuid::Uuid,
        target_username: &str,
        data: &activitypub_federation::config::Data<FederationData>,
    ) -> anyhow::Result<()> {
        let target = data
            .user_repo
            .find_by_username(target_username)
            .await?
            .ok_or_else(|| anyhow::anyhow!("user not found: {}", target_username))?;

        if target.id == local_user_id {
            return Err(anyhow::anyhow!("cannot follow yourself"));
        }

        let follower_actor_url = crate::urls::actor_url(&self.base_url, local_user_id).to_string();
        let target_actor_url = crate::urls::actor_url(&self.base_url, target.id);
        let target_inbox_url = format!("{}/inbox", target_actor_url);
        let follow_id = activity_url(&self.base_url)
            .map_err(|e| anyhow::anyhow!("{e}"))?
            .to_string();

        data.federation_repo
            .add_follower(
                target.id,
                &follower_actor_url,
                FollowerStatus::Accepted,
                &follow_id,
            )
            .await?;

        let target_as_remote = RemoteActor {
            url: target_actor_url.to_string(),
            handle: format!("{}@{}", target.username, data.domain),
            inbox_url: target_inbox_url,
            shared_inbox_url: None,
            display_name: Some(target.username),
            avatar_url: None,
            outbox_url: None,
        };
        data.federation_repo
            .add_following(local_user_id, target_as_remote, &follow_id)
            .await?;

        data.federation_repo
            .update_following_status(
                local_user_id,
                target_actor_url.as_ref(),
                FollowingStatus::Accepted,
            )
            .await?;

        tracing::info!(follower = %local_user_id, followee = %target.id, "local follow");
        Ok(())
    }

    async fn unfollow_local(
        &self,
        local_user_id: uuid::Uuid,
        target_actor_url: &str,
        data: &activitypub_federation::config::Data<FederationData>,
    ) -> anyhow::Result<()> {
        let target_url = Url::parse(target_actor_url)?;
        let target_user_id = crate::urls::extract_user_id_from_url(&target_url)
            .ok_or_else(|| anyhow::anyhow!("invalid local actor URL: {}", target_actor_url))?;

        let local_actor_url = crate::urls::actor_url(&self.base_url, local_user_id).to_string();

        data.federation_repo
            .remove_follower(target_user_id, &local_actor_url)
            .await?;
        data.federation_repo
            .remove_following(local_user_id, target_actor_url)
            .await?;

        tracing::info!(follower = %local_user_id, followee = %target_user_id, "local unfollow");
        Ok(())
    }

    pub async fn backfill_outbox(&self, outbox_url: &str, actor_url: &str) -> anyhow::Result<()> {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(HTTP_FETCH_TIMEOUT_SECS))
            .build()?;
        let data = self.federation_config.to_request_data();
        let actor = url::Url::parse(actor_url)?;

        let root: serde_json::Value = client
            .get(outbox_url)
            .header("Accept", "application/activity+json")
            .send()
            .await?
            .json()
            .await?;

        let first = match root.get("first").and_then(|v| v.as_str()) {
            Some(url) => url.to_string(),
            None => {
                tracing::debug!(outbox = %outbox_url, "outbox has no first page, nothing to backfill");
                return Ok(());
            }
        };

        let mut current_url = first;
        let mut visited = std::collections::HashSet::new();

        loop {
            if !visited.insert(current_url.clone()) {
                tracing::warn!(url = %current_url, "backfill: loop detected, stopping");
                break;
            }

            let page: serde_json::Value = match client
                .get(&current_url)
                .header("Accept", "application/activity+json")
                .send()
                .await
            {
                Ok(resp) => match resp.json().await {
                    Ok(v) => v,
                    Err(e) => {
                        tracing::error!(error = %e, url = %current_url, "backfill: failed to parse page JSON");
                        break;
                    }
                },
                Err(e) => {
                    tracing::error!(error = %e, url = %current_url, "backfill: HTTP request failed");
                    break;
                }
            };

            if let Some(items) = page.get("orderedItems").and_then(|v| v.as_array()) {
                for item in items {
                    let activity_type = item.get("type").and_then(|v| v.as_str()).unwrap_or("");
                    if activity_type != "Create" && activity_type != "Add" {
                        continue;
                    }
                    let object = match item.get("object") {
                        Some(o) if o.is_object() => o.clone(),
                        _ => continue,
                    };
                    let ap_id = match object
                        .get("id")
                        .and_then(|v| v.as_str())
                        .and_then(|s| url::Url::parse(s).ok())
                    {
                        Some(u) => u,
                        None => continue,
                    };
                    if let Err(e) = data.object_handler.on_create(&ap_id, &actor, object).await {
                        tracing::warn!(ap_id = %ap_id, error = %e, "backfill: failed to process item, skipping");
                    }
                }
            }

            match page.get("next").and_then(|v| v.as_str()) {
                Some(next) => current_url = next.to_string(),
                None => break,
            }
        }

        tracing::info!(outbox = %outbox_url, pages = visited.len(), "backfill complete");
        Ok(())
    }

    fn spawn_backfill(&self, owner_user_id: uuid::Uuid, follower_inbox_url: String) {
        let config = self.federation_config.clone();
        let base_url = self.base_url.clone();
        tokio::spawn(async move {
            if let Err(e) = ActivityPubService::run_backfill(
                config,
                base_url,
                owner_user_id,
                follower_inbox_url,
            )
            .await
            {
                tracing::warn!(error = %e, "backfill: task failed");
            }
        });
    }

    async fn run_backfill(
        config: ApFederationConfig,
        base_url: String,
        owner_user_id: uuid::Uuid,
        follower_inbox_url: String,
    ) -> anyhow::Result<()> {
        const BATCH_SIZE: usize = 20;

        let data = config.to_request_data();
        let local_actor = get_local_actor(owner_user_id, &data)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let inbox = Url::parse(&follower_inbox_url)?;

        let mut objects = data
            .object_handler
            .get_local_objects_for_user(owner_user_id)
            .await?;
        objects.reverse(); // oldest first → chronological feed

        let total = objects.len();
        let mut success_count = 0usize;
        let mut failure_count = 0usize;

        for chunk in objects.chunks(BATCH_SIZE) {
            for (ap_id, object_json) in chunk {
                // Use a stable Create activity ID derived from the object's ap_id
                let create_id = Url::parse(&format!(
                    "{}/activities/create/{}",
                    base_url,
                    uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_URL, ap_id.as_str().as_bytes())
                ))?;

                let create = CreateActivity {
                    id: create_id,
                    kind: Default::default(),
                    actor: ObjectId::from(local_actor.ap_id.clone()),
                    object: object_json.clone(),
                    to: vec![],
                    cc: vec![],
                    bto: vec![],
                    bcc: vec![],
                };

                let sends = SendActivityTask::prepare(
                    &WithContext::new_default(create),
                    &local_actor,
                    vec![inbox.clone()],
                    &data,
                )
                .await?;
                let failures = send_with_retry(sends, &data).await;
                if failures.is_empty() {
                    success_count += 1;
                } else {
                    failure_count += 1;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(BATCH_FETCH_SLEEP_MS)).await;
        }

        tracing::info!(
            user_id = %owner_user_id,
            follower = %follower_inbox_url,
            sent = success_count,
            failed = failure_count,
            total = total,
            "backfill complete"
        );
        Ok(())
    }
}
#[cfg(test)]
#[path = "tests/service.rs"]
mod tests;
