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
            return Err(Error::bad_request(
                "Undo actor does not match inner activity actor",
            ));
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
            .and_then(|type_value| type_value.as_str())
            .unwrap_or("");
        match obj_type {
            "Follow" => handle_undo_follow(self.actor.inner(), &self.object, data).await?,
            "Add" => handle_undo_add(self.actor.inner(), &self.object, data).await?,
            "Like" => handle_undo_like(self.actor.inner(), &self.object, data).await?,
            "Announce" => handle_undo_announce(self.actor.inner(), &self.object, data).await?,
            "Block" => handle_undo_block(self.actor.inner(), &self.object, data).await?,
            other => {
                tracing::debug!(kind = %other, "ignoring Undo of unknown activity type");
            }
        }
        Ok(())
    }
}

async fn handle_undo_follow(
    actor: &Url,
    object: &serde_json::Value,
    data: &Data<FederationData>,
) -> Result<(), Error> {
    if let Some(obj_url) = object.get("object").and_then(|inner| inner.as_str())
        && let Ok(url) = Url::parse(obj_url)
        && let Some(user_id) = data.url_scheme.extract_user_id(&url)
    {
        data.follow_repo
            .remove_follower(user_id, actor.as_str())
            .await?;
    }

    data.object_handler.on_actor_removed(actor).await?;
    tracing::info!(actor = %actor, "unfollowed");
    Ok(())
}

async fn handle_undo_add(
    actor: &Url,
    object: &serde_json::Value,
    data: &Data<FederationData>,
) -> Result<(), Error> {
    let ap_id_str = object
        .get("object")
        .and_then(|inner| inner.get("id"))
        .and_then(|id| id.as_str())
        .or_else(|| object.get("id").and_then(|id| id.as_str()));

    if let Some(ap_id_str) = ap_id_str
        && let Ok(ap_id) = Url::parse(ap_id_str)
    {
        data.object_handler.on_delete(&ap_id, actor).await?;
        tracing::info!(ap_id = %ap_id_str, "undo Add (watchlist remove)");
    }
    Ok(())
}

async fn handle_undo_like(
    actor: &Url,
    object: &serde_json::Value,
    data: &Data<FederationData>,
) -> Result<(), Error> {
    if let Some(obj_url_str) = object.get("object").and_then(|inner| inner.as_str())
        && let Ok(obj_url) = Url::parse(obj_url_str)
        && obj_url.host_str().unwrap_or("") == data.domain
    {
        data.object_handler
            .on_unlike(&obj_url, actor)
            .await
            .unwrap_or_else(|e| tracing::warn!(error = %e, "failed to process unlike"));
    }

    tracing::info!(actor = %actor, "received Undo(Like)");
    Ok(())
}

async fn handle_undo_announce(
    actor: &Url,
    object: &serde_json::Value,
    data: &Data<FederationData>,
) -> Result<(), Error> {
    // Remove the boost record so announce counts stay accurate.
    let activity_id = object.get("id").and_then(|v| v.as_str()).unwrap_or("");
    let object_url_str = object.get("object").and_then(|v| v.as_str()).unwrap_or("");

    if !activity_id.is_empty()
        && let Err(e) = data
            .actor_repo
            .remove_announce(activity_id, actor.as_str())
            .await
    {
        tracing::warn!(error = %e, activity_id, "failed to remove announce record");
    }

    if let Ok(obj_url) = Url::parse(object_url_str)
        && obj_url.host_str().unwrap_or("") == data.domain
    {
        data.object_handler
            .on_announce_removed(&obj_url, actor)
            .await
            .unwrap_or_else(|e| {
                tracing::warn!(error = %e, "failed to process Undo(Announce)");
            });
    }

    tracing::info!(actor = %actor, "received Undo(Announce)");
    Ok(())
}

async fn handle_undo_block(
    actor: &Url,
    object: &serde_json::Value,
    data: &Data<FederationData>,
) -> Result<(), Error> {
    if let Some(obj_url) = object.get("object").and_then(|inner| inner.as_str())
        && let Ok(url) = Url::parse(obj_url)
        && let Some(user_id) = data.url_scheme.extract_user_id(&url)
        && let Err(error) = data
            .blocklist_repo
            .remove_blocked_actor(user_id, actor.as_str())
            .await
    {
        tracing::debug!(%error, "block record already removed");
    }

    tracing::info!(
        actor = %actor,
        "received Undo(Block) — removed from blocklist"
    );
    Ok(())
}
