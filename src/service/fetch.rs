use url::Url;

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
}
