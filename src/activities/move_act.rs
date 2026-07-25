use activitypub_federation::{
    activity_sending::SendActivityTask, config::Data, fetch::object_id::ObjectId,
    protocol::context::WithContext, traits::Activity,
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

    fn id(&self) -> &Url {
        &self.id
    }
    fn actor(&self) -> &Url {
        self.actor.inner()
    }

    async fn verify(&self, _data: &Data<Self::DataType>) -> Result<(), Self::Error> {
        if &self.object != self.actor.inner() {
            return Err(Error::bad_request("Move object must be the actor itself"));
        }
        Ok(())
    }

    async fn receive(self, data: &Data<Self::DataType>) -> Result<(), Self::Error> {
        if check_guards(&self.id, self.actor.inner(), data).await? {
            return Ok(());
        }

        let target = ObjectId::<DbActor>::from(self.target.clone())
            .dereference(data)
            .await?;
        let old_url = self.object.as_str();
        if !target.also_known_as.iter().any(|a| a == old_url) {
            return Err(Error::bad_request(
                "Move target alsoKnownAs does not reference old actor",
            ));
        }

        let affected = data
            .follow_repo
            .migrate_follower_actor(old_url, self.target.as_str())
            .await?;
        let affected_count = affected.len();

        // Spawn re-follows in the background — do NOT await them inside receive()
        // to avoid blocking the inbox handler while making outbound HTTP requests.
        let target_inbox = target.inbox_url.clone();
        let target_url = self.target.clone();
        let base_url = data.base_url.clone();
        let data_clone = data.clone();
        tokio::spawn(async move {
            for local_user_id in &affected {
                if let Err(e) = send_refollow(
                    *local_user_id,
                    &target_url,
                    &target_inbox,
                    &base_url,
                    &data_clone,
                )
                .await
                {
                    tracing::warn!(
                        error = %e,
                        %local_user_id,
                        "Move: re-follow failed"
                    );
                }
            }
        });

        tracing::info!(
            actor = %self.actor.inner(),
            target = %self.target,
            affected = affected_count,
            "received Move — migrated follower relationships, re-follows spawned"
        );
        Ok(())
    }
}

async fn send_refollow(
    local_user_id: uuid::Uuid,
    new_target_url: &Url,
    new_target_inbox: &Url,
    base_url: &str,
    data: &Data<FederationData>,
) -> anyhow::Result<()> {
    let local_actor = crate::actors::get_local_actor(local_user_id, data).await?;
    let follow_id = data.url_scheme.activity_url(base_url)?;

    let follow = FollowActivity {
        id: follow_id,
        kind: Default::default(),
        actor: ObjectId::from(local_actor.ap_id.clone()),
        object: ObjectId::from(new_target_url.clone()),
    };

    let sends = SendActivityTask::prepare(
        &WithContext::new_default(follow),
        &local_actor,
        vec![new_target_inbox.clone()],
        data,
    )
    .await?;

    for send in sends {
        if let Err(e) = send.sign_and_send(data).await {
            tracing::warn!(
                error = %e,
                %local_user_id,
                "Move: re-follow delivery failed"
            );
        }
    }
    Ok(())
}
