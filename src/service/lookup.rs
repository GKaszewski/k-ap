use url::Url;

use crate::{actors::DbActor, data::FederationData, repository::BlockedDomain};

use super::ActivityPubService;

struct ParsedHandle<'a> {
    username: &'a str,
    domain: &'a str,
}

fn parse_handle(handle: &str) -> anyhow::Result<ParsedHandle<'_>> {
    let normalized = handle.trim_start_matches('@');
    let separator_index = normalized
        .rfind('@')
        .ok_or_else(|| anyhow::anyhow!("handle must be user@domain"))?;
    Ok(ParsedHandle {
        username: &normalized[..separator_index],
        domain: &normalized[separator_index + 1..],
    })
}

fn webfinger_url(handle: &ParsedHandle<'_>) -> String {
    format!(
        "https://{}/.well-known/webfinger?resource=acct:{}@{}",
        handle.domain, handle.username, handle.domain
    )
}

async fn fetch_webfinger(url: &str) -> anyhow::Result<serde_json::Value> {
    let parsed = Url::parse(url)?;
    crate::security::validate_url(&parsed).await?;
    Ok(reqwest::Client::new()
        .get(url)
        .header("Accept", "application/jrd+json, application/json")
        .send()
        .await?
        .json()
        .await?)
}

fn extract_actor_href(webfinger: &serde_json::Value) -> anyhow::Result<String> {
    webfinger["links"]
        .as_array()
        .and_then(|links| {
            links.iter().find(|link| {
                link["rel"].as_str() == Some("self")
                    && link["type"].as_str() == Some(crate::urls::AP_CONTENT_TYPE)
            })
        })
        .and_then(|link| link["href"].as_str())
        .map(|href| href.to_owned())
        .ok_or_else(|| anyhow::anyhow!("no self link in WebFinger response"))
}

impl ActivityPubService {
    // ── Pass-through wrappers ───────────────────────────────────────────

    pub async fn mark_follower_accepted(
        &self,
        user_id: uuid::Uuid,
        actor_url: &str,
    ) -> anyhow::Result<()> {
        let data = self.federation_config.to_request_data();
        data.follow_repo
            .update_follower_status(
                user_id,
                actor_url,
                crate::repository::FollowerStatus::Accepted,
            )
            .await
    }

    pub async fn mark_follower_rejected(
        &self,
        user_id: uuid::Uuid,
        actor_url: &str,
    ) -> anyhow::Result<()> {
        let data = self.federation_config.to_request_data();
        data.follow_repo.remove_follower(user_id, actor_url).await
    }

    pub async fn add_blocked_domain(
        &self,
        domain: &str,
        reason: Option<&str>,
    ) -> anyhow::Result<()> {
        let data = self.federation_config.to_request_data();
        data.blocklist_repo.add_blocked_domain(domain, reason).await
    }

    pub async fn remove_blocked_domain(&self, domain: &str) -> anyhow::Result<()> {
        let data = self.federation_config.to_request_data();
        data.blocklist_repo.remove_blocked_domain(domain).await
    }

    pub async fn get_blocked_domains(&self) -> anyhow::Result<Vec<BlockedDomain>> {
        let data = self.federation_config.to_request_data();
        data.blocklist_repo.get_blocked_domains().await
    }

    // ── WebFinger / actor resolution ────────────────────────────────────

    pub async fn lookup_actor_by_handle(
        &self,
        handle: &str,
    ) -> anyhow::Result<crate::user::LookedUpActor> {
        tracing::info!(handle, "looking up remote actor");
        let data = self.federation_config.to_request_data();
        let actor = self
            .webfinger_https(handle, &data)
            .await
            .inspect_err(|error| tracing::warn!(handle, %error, "actor lookup failed"))?;
        let domain = actor.ap_id.host_str().unwrap_or("").to_string();
        tracing::info!(handle = format!("{}@{}", actor.username, domain), ap_url = %actor.ap_id, "remote actor resolved");

        Ok(crate::user::LookedUpActor {
            handle: format!("{}@{}", actor.username, domain),
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

    pub(super) async fn webfinger_https(
        &self,
        handle: &str,
        data: &activitypub_federation::config::Data<FederationData>,
    ) -> anyhow::Result<DbActor> {
        let parsed_handle = parse_handle(handle)?;
        let url = webfinger_url(&parsed_handle);
        tracing::debug!(handle, webfinger_url = %url, "resolving webfinger");

        let webfinger_response = fetch_webfinger(&url).await?;
        let actor_href = extract_actor_href(&webfinger_response)?;

        tracing::debug!(handle, actor_href, "webfinger resolved, fetching actor");
        let actor: DbActor =
            activitypub_federation::fetch::object_id::ObjectId::from(Url::parse(&actor_href)?)
                .dereference(data)
                .await?;
        Ok(actor)
    }
}
