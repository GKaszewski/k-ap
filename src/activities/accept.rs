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
            return Err(Error::bad_request(anyhow::anyhow!(
                "Accept actor does not match Follow target"
            )));
        }
        Ok(())
    }

    async fn receive(self, data: &Data<Self::DataType>) -> Result<(), Self::Error> {
        if check_guards(&self.id, self.actor.inner(), data).await? {
            return Ok(());
        }
        let local_user_id = crate::urls::extract_user_id_from_url(self.object.actor.inner())
            .ok_or_else(|| Error::bad_request(anyhow::anyhow!("invalid actor URL in Follow")))?;
        data.follow_repo
            .update_following_status(
                local_user_id,
                self.actor.inner().as_str(),
                FollowingStatus::Accepted,
            )
            .await?;
        tracing::info!(remote_actor = %self.actor.inner(), "follow accepted by remote");
        Ok(())
    }
}
