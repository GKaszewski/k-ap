use activitypub_federation::{
    activity_sending::SendActivityTask,
    config::Data,
    fetch::object_id::ObjectId,
    protocol::context::WithContext,
    traits::Activity,
};
use serde::{Deserialize, Serialize};
use url::Url;

use crate::actors::DbActor;
use crate::data::FederationData;
use crate::error::Error;

use super::follow::FollowActivity;
use super::helpers::check_guards;

#[derive(Clone, Default, Debug, Serialize, Deserialize)]
#[serde(rename = "Move")]
pub struct MoveType;

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MoveActivity {
    pub(crate) id: Url,
    #[serde(rename = "type", default)]
    pub(crate) kind: MoveType,
    pub(crate) actor: ObjectId<DbActor>,
    pub(crate) object: Url,
    pub(crate) target: Url,
}

#[async_trait::async_trait]
impl Activity for MoveActivity {
    type DataType = FederationData;
    type Error = Error;

    fn id(&self) -> &Url { &self.id }
    fn actor(&self) -> &Url { self.actor.inner() }

    async fn verify(&self, _data: &Data<Self::DataType>) -> Result<(), Self::Error> {
        if &self.object != self.actor.inner() {
            return Err(Error::bad_request(anyhow::anyhow!("Move object must be the actor itself")));
        }
        Ok(())
    }

    async fn receive(self, data: &Data<Self::DataType>) -> Result<(), Self::Error> {
        if check_guards(&self.id, self.actor.inner(), data).await? {
            return Ok(());
        }
        let target = ObjectId::<DbActor>::from(self.target.clone())
            .dereference(data)
            .await
            .map_err(|e| Error::from(anyhow::anyhow!("{e}")))?;
        if target.also_known_as.as_deref() != Some(self.object.as_str()) {
            return Err(Error::bad_request(anyhow::anyhow!(
                "Move target alsoKnownAs does not reference old actor"
            )));
        }
        let affected = data
            .follow_repo
            .migrate_follower_actor(self.object.as_str(), self.target.as_str())
            .await
            .map_err(|e| Error::from(anyhow::anyhow!("{e}")))?;
        let affected_count = affected.len();
        for local_user_id in &affected {
            let local_actor = match crate::actors::get_local_actor(*local_user_id, data).await {
                Ok(a) => a,
                Err(e) => { tracing::warn!(error = %e, %local_user_id, "Move: failed to load local actor"); continue; }
            };
            let follow_id = match crate::urls::activity_url(&data.base_url) {
                Ok(u) => u,
                Err(e) => { tracing::warn!(error = %e, "Move: failed to generate follow activity URL"); continue; }
            };
            let follow = FollowActivity {
                id: follow_id,
                kind: Default::default(),
                actor: ObjectId::from(local_actor.ap_id.clone()),
                object: ObjectId::from(self.target.clone()),
            };
            let sends = match SendActivityTask::prepare(
                &WithContext::new_default(follow),
                &local_actor,
                vec![target.inbox_url.clone()],
                data,
            ).await {
                Ok(s) => s,
                Err(e) => { tracing::warn!(error = %e, "Move: failed to prepare re-follow"); continue; }
            };
            for send in sends {
                if let Err(e) = send.sign_and_send(data).await {
                    tracing::warn!(error = %e, %local_user_id, "Move: re-follow delivery failed");
                }
            }
        }
        tracing::info!(
            actor = %self.actor.inner(),
            target = %self.target,
            affected = affected_count,
            "received Move — migrated follower relationships"
        );
        Ok(())
    }
}
