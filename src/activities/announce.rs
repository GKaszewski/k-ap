use activitypub_federation::{
    config::Data,
    fetch::object_id::ObjectId,
    protocol::verification::verify_domains_match,
    traits::Activity,
};
use serde::{Deserialize, Serialize};
use url::Url;

use crate::actors::DbActor;
use crate::data::FederationData;
use crate::error::Error;

use super::helpers::check_guards;

#[derive(Clone, Default, Debug, Serialize, Deserialize)]
#[serde(rename = "Announce")]
pub struct AnnounceType;

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AnnounceActivity {
    pub(crate) id: Url,
    #[serde(rename = "type", default)]
    pub(crate) kind: AnnounceType,
    pub(crate) actor: ObjectId<DbActor>,
    pub(crate) object: Url,
    pub(crate) published: Option<chrono::DateTime<chrono::Utc>>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub(crate) to: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub(crate) cc: Vec<String>,
}

#[async_trait::async_trait]
impl Activity for AnnounceActivity {
    type DataType = FederationData;
    type Error = Error;

    fn id(&self) -> &Url { &self.id }
    fn actor(&self) -> &Url { self.actor.inner() }

    async fn verify(&self, _data: &Data<Self::DataType>) -> Result<(), Self::Error> {
        verify_domains_match(&self.id, self.actor.inner())?;
        Ok(())
    }

    async fn receive(self, data: &Data<Self::DataType>) -> Result<(), Self::Error> {
        if check_guards(&self.id, self.actor.inner(), data).await? {
            return Ok(());
        }
        if self.object.host_str().unwrap_or("") != data.domain {
            data.object_handler
                .on_announce_of_remote(&self.object, self.actor.inner())
                .await
                .unwrap_or_else(|e| tracing::warn!(error = %e, "failed to process cross-server announce"));
            tracing::debug!(actor = %self.actor.inner(), object = %self.object, "received Announce of non-local object");
            return Ok(());
        }
        data.actor_repo
            .add_announce(
                self.id.as_str(),
                self.object.as_str(),
                self.actor.inner().as_str(),
                self.published.unwrap_or_else(chrono::Utc::now),
            )
            .await?;
        data.object_handler
            .on_announce_received(&self.object, self.actor.inner())
            .await
            .unwrap_or_else(|e| tracing::warn!(error = %e, "failed to process announce notification"));
        tracing::info!(actor = %self.actor.inner(), object = %self.object, "received announce");
        Ok(())
    }
}
