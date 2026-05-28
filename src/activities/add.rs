use activitypub_federation::{config::Data, fetch::object_id::ObjectId, traits::Activity};
use serde::{Deserialize, Serialize};
use url::Url;

use crate::actors::DbActor;
use crate::data::FederationData;
use crate::error::Error;

use super::helpers::check_guards;

#[derive(Clone, Default, Debug, Serialize, Deserialize)]
#[serde(rename = "Add")]
pub struct AddType;

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AddActivity {
    pub(crate) id: Url,
    #[serde(rename = "type", default)]
    pub(crate) kind: AddType,
    pub(crate) actor: ObjectId<DbActor>,
    pub(crate) object: serde_json::Value,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub(crate) to: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub(crate) cc: Vec<String>,
}

#[async_trait::async_trait]
impl Activity for AddActivity {
    type DataType = FederationData;
    type Error = Error;

    fn id(&self) -> &Url {
        &self.id
    }
    fn actor(&self) -> &Url {
        self.actor.inner()
    }

    async fn verify(&self, _data: &Data<Self::DataType>) -> Result<(), Self::Error> {
        if let Some(attributed_to) = self.object.get("attributedTo").and_then(|v| v.as_str())
            && let Ok(attributed_url) = Url::parse(attributed_to)
            && &attributed_url != self.actor.inner()
        {
            return Err(Error::bad_request(anyhow::anyhow!(
                "Add actor does not match object attributedTo"
            )));
        }
        Ok(())
    }

    async fn receive(self, data: &Data<Self::DataType>) -> Result<(), Self::Error> {
        if check_guards(&self.id, self.actor.inner(), data).await? {
            return Ok(());
        }
        let ap_id = self.id.clone();
        let actor_url = self.actor.inner().clone();
        data.object_handler
            .on_create(&ap_id, &actor_url, self.object)
            .await
            .map_err(|e| Error::from(anyhow::anyhow!(e)))?;
        tracing::info!(actor = %actor_url, "received Add activity");
        Ok(())
    }
}
