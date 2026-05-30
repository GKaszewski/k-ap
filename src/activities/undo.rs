use activitypub_federation::{
    config::Data, fetch::object_id::ObjectId, kinds::activity::UndoType, traits::Activity,
};
use serde::{Deserialize, Serialize};
use url::Url;

use crate::actors::DbActor;
use crate::data::FederationData;
use crate::error::Error;

use super::helpers::check_guards;

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UndoActivity {
    pub(crate) id: Url,
    #[serde(rename = "type", default)]
    pub(crate) kind: UndoType,
    pub(crate) actor: ObjectId<DbActor>,
    pub(crate) object: serde_json::Value,
}

#[async_trait::async_trait]
impl Activity for UndoActivity {
    type DataType = FederationData;
    type Error = Error;

    fn id(&self) -> &Url {
        &self.id
    }
    fn actor(&self) -> &Url {
        self.actor.inner()
    }

    async fn verify(&self, _data: &Data<Self::DataType>) -> Result<(), Self::Error> {
        if let Some(inner_actor) = self.object.get("actor").and_then(|v| v.as_str())
            && inner_actor != self.actor.inner().as_str()
        {
            return Err(Error::bad_request(anyhow::anyhow!(
                "Undo actor does not match inner activity actor"
            )));
        }
        Ok(())
    }

    async fn receive(self, data: &Data<Self::DataType>) -> Result<(), Self::Error> {
        if check_guards(&self.id, self.actor.inner(), data).await? {
            return Ok(());
        }
        let obj_type = self
            .object
            .get("type")
            .and_then(|t| t.as_str())
            .unwrap_or("");
        match obj_type {
            "Follow" => {
                if let Some(obj_url) = self.object.get("object").and_then(|o| o.as_str())
                    && let Ok(url) = Url::parse(obj_url)
                    && let Some(user_id) = crate::urls::extract_user_id_from_url(&url)
                {
                    data.follow_repo
                        .remove_follower(user_id, self.actor.inner().as_str())
                        .await?;
                }
                data.object_handler
                    .on_actor_removed(self.actor.inner())
                    .await
                    .map_err(|e| Error::from(anyhow::anyhow!(e)))?;
                tracing::info!(actor = %self.actor.inner(), "unfollowed");
            }
            "Add" => {
                let ap_id_str = self
                    .object
                    .get("object")
                    .and_then(|o| o.get("id"))
                    .and_then(|id| id.as_str())
                    .or_else(|| self.object.get("id").and_then(|id| id.as_str()));
                if let Some(ap_id_str) = ap_id_str
                    && let Ok(ap_id) = Url::parse(ap_id_str)
                {
                    data.object_handler
                        .on_delete(&ap_id, self.actor.inner())
                        .await
                        .map_err(|e| Error::from(anyhow::anyhow!(e)))?;
                    tracing::info!(ap_id = %ap_id_str, "undo Add (watchlist remove)");
                }
            }
            "Like" => {
                if let Some(obj_url_str) = self.object.get("object").and_then(|o| o.as_str())
                    && let Ok(obj_url) = Url::parse(obj_url_str)
                    && obj_url.host_str().unwrap_or("") == data.domain
                {
                    data.object_handler
                        .on_unlike(&obj_url, self.actor.inner())
                        .await
                        .unwrap_or_else(|e| tracing::warn!(error = %e, "failed to process unlike"));
                }
                tracing::info!(actor = %self.actor.inner(), "received Undo(Like)");
            }
            "Announce" => {
                // Remove the boost record so announce counts stay accurate.
                let activity_id = self.object.get("id").and_then(|v| v.as_str()).unwrap_or("");
                let object_url_str = self
                    .object
                    .get("object")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");

                if !activity_id.is_empty()
                    && let Err(e) = data
                        .actor_repo
                        .remove_announce(activity_id, self.actor.inner().as_str())
                        .await
                {
                    tracing::warn!(error = %e, activity_id, "failed to remove announce record");
                }

                if let Ok(obj_url) = Url::parse(object_url_str)
                    && obj_url.host_str().unwrap_or("") == data.domain
                {
                    data.object_handler
                        .on_announce_removed(&obj_url, self.actor.inner())
                        .await
                        .unwrap_or_else(|e| {
                            tracing::warn!(error = %e, "failed to process Undo(Announce)");
                        });
                }
                tracing::info!(actor = %self.actor.inner(), "received Undo(Announce)");
            }
            "Block" => {
                if let Some(obj_url) = self.object.get("object").and_then(|o| o.as_str())
                    && let Ok(url) = Url::parse(obj_url)
                    && let Some(user_id) = crate::urls::extract_user_id_from_url(&url)
                {
                    let _ = data
                        .blocklist_repo
                        .remove_blocked_actor(user_id, self.actor.inner().as_str())
                        .await;
                }
                tracing::info!(
                    actor = %self.actor.inner(),
                    "received Undo(Block) — removed from blocklist"
                );
            }
            other => {
                tracing::debug!(kind = %other, "ignoring Undo of unknown activity type");
            }
        }
        Ok(())
    }
}
