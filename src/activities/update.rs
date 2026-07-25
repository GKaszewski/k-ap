use activitypub_federation::{
    config::Data, fetch::object_id::ObjectId, kinds::activity::UpdateType, traits::Activity,
};
use serde::{Deserialize, Serialize};
use url::Url;

use crate::actors::DbActor;
use crate::data::FederationData;
use crate::error::Error;

use super::helpers::{
    check_guards, extract_and_dispatch_mentions, extract_object_ap_id, verify_attributed_to,
};

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateActivity {
    pub(crate) id: Url,
    #[serde(rename = "type", default)]
    pub(crate) kind: UpdateType,
    pub(crate) actor: ObjectId<DbActor>,
    pub(crate) object: serde_json::Value,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub(crate) to: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub(crate) cc: Vec<String>,
}

#[async_trait::async_trait]
impl Activity for UpdateActivity {
    type DataType = FederationData;
    type Error = Error;

    fn id(&self) -> &Url {
        &self.id
    }
    fn actor(&self) -> &Url {
        self.actor.inner()
    }

    async fn verify(&self, _data: &Data<Self::DataType>) -> Result<(), Self::Error> {
        verify_attributed_to(&self.object, self.actor.inner(), "Update")
    }

    async fn receive(self, data: &Data<Self::DataType>) -> Result<(), Self::Error> {
        if check_guards(&self.id, self.actor.inner(), data).await? {
            return Ok(());
        }
        let ap_id = extract_object_ap_id(&self.object, &self.id);
        let actor_url = self.actor.inner().clone();
        extract_and_dispatch_mentions(&ap_id, &actor_url, &self.object, data).await;
        data.object_handler
            .on_update(&ap_id, &actor_url, self.object)
            .await?;
        tracing::info!(actor = %actor_url, "received update activity");
        Ok(())
    }
}
