use activitypub_federation::{
    config::Data, fetch::object_id::ObjectId, protocol::verification::verify_domains_match,
    traits::Activity,
};
use serde::{Deserialize, Serialize};
use url::Url;

use crate::actors::DbActor;
use crate::data::FederationData;
use crate::error::Error;

use super::helpers::check_guards;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename = "Like")]
pub struct LikeType;

impl Default for LikeType {
    fn default() -> Self {
        Self
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LikeActivity {
    pub id: Url,
    #[serde(rename = "type")]
    pub kind: LikeType,
    pub actor: ObjectId<DbActor>,
    pub object: Url,
}

#[async_trait::async_trait]
impl Activity for LikeActivity {
    type DataType = FederationData;
    type Error = Error;

    fn id(&self) -> &Url {
        &self.id
    }
    fn actor(&self) -> &Url {
        self.actor.inner()
    }

    async fn verify(&self, _data: &Data<Self::DataType>) -> Result<(), Self::Error> {
        verify_domains_match(&self.id, self.actor.inner())?;
        Ok(())
    }

    async fn receive(self, data: &Data<Self::DataType>) -> Result<(), Self::Error> {
        if check_guards(&self.id, self.actor.inner(), data).await? {
            return Ok(());
        }
        if self.object.host_str().unwrap_or("") != data.domain {
            return Ok(());
        }
        data.object_handler
            .on_like(&self.object, self.actor.inner())
            .await?;
        tracing::info!(actor = %self.actor.inner(), object = %self.object, "received like");
        Ok(())
    }
}
