use activitypub_federation::{
    config::Data, fetch::object_id::ObjectId, kinds::activity::DeleteType, traits::Activity,
};
use serde::{Deserialize, Serialize};
use url::Url;

use crate::actors::DbActor;
use crate::data::FederationData;
use crate::error::Error;

use super::helpers::check_guards;

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DeleteActivity {
    pub(crate) id: Url,
    #[serde(rename = "type", default)]
    pub(crate) kind: DeleteType,
    pub(crate) actor: ObjectId<DbActor>,
    pub(crate) object: serde_json::Value,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub(crate) to: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub(crate) cc: Vec<String>,
}

#[async_trait::async_trait]
impl Activity for DeleteActivity {
    type DataType = FederationData;
    type Error = Error;

    fn id(&self) -> &Url {
        &self.id
    }
    fn actor(&self) -> &Url {
        self.actor.inner()
    }

    async fn verify(&self, _data: &Data<Self::DataType>) -> Result<(), Self::Error> {
        let actor_domain = self.actor.inner().host_str().unwrap_or("");
        let object_domain = match &self.object {
            serde_json::Value::String(s) => Url::parse(s)
                .ok()
                .and_then(|u| u.host_str().map(|h| h.to_string()))
                .unwrap_or_default(),
            serde_json::Value::Object(o) => o
                .get("id")
                .and_then(|v| v.as_str())
                .and_then(|s| Url::parse(s).ok())
                .and_then(|u| u.host_str().map(|h| h.to_string()))
                .unwrap_or_default(),
            _ => String::new(),
        };
        if !object_domain.is_empty() && actor_domain != object_domain {
            return Err(Error::bad_request(anyhow::anyhow!(
                "Delete actor domain does not match object domain"
            )));
        }
        Ok(())
    }

    async fn receive(self, data: &Data<Self::DataType>) -> Result<(), Self::Error> {
        if check_guards(&self.id, self.actor.inner(), data).await? {
            return Ok(());
        }
        let actor_url = self.actor.inner().clone();
        let object_url_str = match &self.object {
            serde_json::Value::String(s) => s.clone(),
            serde_json::Value::Object(o) => o
                .get("id")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .unwrap_or_default(),
            _ => String::new(),
        };
        let Ok(object_url) = Url::parse(&object_url_str) else {
            tracing::warn!(actor = %actor_url, "Delete has unparseable object, ignoring");
            return Ok(());
        };
        if object_url == *self.actor.inner() {
            data.object_handler
                .on_actor_removed(&actor_url)
                .await
                .map_err(|e| Error::from(anyhow::anyhow!(e)))?;
            tracing::info!(actor = %actor_url, "received Delete(actor) — remote account deleted");
            return Ok(());
        }
        data.object_handler
            .on_delete(&object_url, &actor_url)
            .await
            .map_err(|e| Error::from(anyhow::anyhow!(e)))?;
        tracing::info!(object = %object_url, "received Delete(note)");
        Ok(())
    }
}
