use activitypub_federation::{
    config::Data, fetch::object_id::ObjectId, kinds::activity::AcceptType, traits::Activity,
};
use serde::{Deserialize, Serialize};
use url::Url;

use crate::actors::DbActor;
use crate::data::FederationData;
use crate::error::Error;
use crate::repository::FollowingStatus;

use super::follow::FollowActivity;
use super::helpers::check_guards;

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AcceptActivity {
    pub(crate) id: Url,
    #[serde(rename = "type", default)]
    pub(crate) kind: AcceptType,
    pub(crate) actor: ObjectId<DbActor>,
    pub(crate) object: FollowActivity,
}

#[async_trait::async_trait]
impl Activity for AcceptActivity {
    type DataType = FederationData;
    type Error = Error;

    fn id(&self) -> &Url {
        &self.id
    }
    fn actor(&self) -> &Url {
        self.actor.inner()
    }

    async fn verify(&self, _data: &Data<Self::DataType>) -> Result<(), Self::Error> {
        if self.actor.inner() != self.object.object.inner() {
            return Err(Error::bad_request(
                "Accept actor does not match Follow target",
            ));
        }
        Ok(())
    }

    async fn receive(self, data: &Data<Self::DataType>) -> Result<(), Self::Error> {
        if check_guards(&self.id, self.actor.inner(), data).await? {
            return Ok(());
        }
        let local_user_id = data
            .url_scheme
            .extract_user_id(self.object.actor.inner())
            .ok_or_else(|| Error::bad_request("invalid actor URL in Follow"))?;
        let remote_actor_url = self.actor.inner().as_str().to_string();
        data.follow_repo
            .update_following_status(local_user_id, &remote_actor_url, FollowingStatus::Accepted)
            .await?;
        tracing::info!(remote_actor = %remote_actor_url, "follow accepted by remote");

        if let Some(publisher) = &data.event_publisher {
            let outbox_url = data
                .actor_repo
                .get_remote_actor(&remote_actor_url)
                .await
                .ok()
                .flatten()
                .and_then(|actor| actor.outbox_url);
            if let Err(error) = publisher
                .publish(crate::data::FederationEvent::OutboundFollowAccepted {
                    local_user_id,
                    remote_actor_url,
                    outbox_url,
                })
                .await
            {
                tracing::warn!(%error, "failed to publish OutboundFollowAccepted event");
            }
        }
        Ok(())
    }
}
