use activitypub_federation::{
    config::Data, fetch::object_id::ObjectId, kinds::activity::RejectType, traits::Activity,
};
use serde::{Deserialize, Serialize};
use url::Url;

use crate::actors::DbActor;
use crate::data::FederationData;
use crate::error::Error;

use super::follow::FollowActivity;
use super::helpers::check_guards;

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RejectActivity {
    pub(crate) id: Url,
    #[serde(rename = "type", default)]
    pub(crate) kind: RejectType,
    pub(crate) actor: ObjectId<DbActor>,
    pub(crate) object: FollowActivity,
}

#[async_trait::async_trait]
impl Activity for RejectActivity {
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
                "Reject actor does not match Follow target",
            ));
        }
        Ok(())
    }

    async fn receive(self, data: &Data<Self::DataType>) -> Result<(), Self::Error> {
        if check_guards(&self.id, self.actor.inner(), data).await? {
            return Ok(());
        }
        if let Some(user_id) = data.url_scheme.extract_user_id(self.object.actor.inner()) {
            data.follow_repo
                .remove_following(user_id, self.actor.inner().as_str())
                .await?;
        }
        tracing::info!(actor = %self.actor.inner(), "follow rejected");
        Ok(())
    }
}
