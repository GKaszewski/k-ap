use activitypub_federation::fetch::object_id::ObjectId;
use url::Url;

use crate::actors::DbActor;
use crate::repository::RemoteActor;

use super::ActivityPubService;

impl ActivityPubService {
    /// Fetch a remote ActivityPub resource with HTTP Signatures.
    ///
    /// Requires `signed_fetch_actor_id` to have been set on the builder.
    /// Returns the raw JSON value of the remote resource.
    pub async fn signed_fetch(&self, url: &Url) -> anyhow::Result<serde_json::Value> {
        let data = self.federation_config.to_request_data();
        let res = activitypub_federation::fetch::fetch_object_http::<
            crate::data::FederationData,
            serde_json::Value,
        >(url, &data)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
        Ok(res.object)
    }

    /// Get a cached remote actor, re-fetching from origin if stale.
    ///
    /// Returns `None` if the actor has never been seen. Staleness is
    /// determined by `actor_cache_ttl_secs` (builder config).
    pub async fn get_or_refresh_remote_actor(
        &self,
        actor_url: &str,
    ) -> anyhow::Result<Option<RemoteActor>> {
        let data = self.federation_config.to_request_data();
        let cached = data.actor_repo.get_remote_actor(actor_url).await?;
        if let Some(ref actor) = cached {
            let is_fresh = actor
                .fetched_at
                .map(|t| {
                    let age = chrono::Utc::now().signed_duration_since(t);
                    age < chrono::Duration::from_std(data.actor_cache_ttl).unwrap_or_default()
                })
                .unwrap_or_else(|| {
                    tracing::debug!(actor_url, "fetched_at is None, treating as stale — consider populating fetched_at in get_remote_actor()");
                    false
                });
            tracing::debug!(
                actor_url,
                cache_hit = true,
                fresh = is_fresh,
                "actor cache lookup"
            );
            if is_fresh {
                return Ok(cached);
            }
        } else {
            tracing::debug!(actor_url, cache_hit = false, "actor cache lookup");
        }
        let url = match Url::parse(actor_url) {
            Ok(u) => u,
            Err(_) => return Ok(cached),
        };
        match ObjectId::<DbActor>::from(url)
            .dereference_forced(&data)
            .await
        {
            Ok(_) => Ok(data.actor_repo.get_remote_actor(actor_url).await?),
            Err(e) => {
                tracing::warn!(actor_url, error = %e, "re-fetch failed, using stale cache");
                Ok(cached)
            }
        }
    }
}
