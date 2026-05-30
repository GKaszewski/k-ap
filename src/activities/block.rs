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

#[derive(Clone, Default, Debug, Serialize, Deserialize)]
#[serde(rename = "Block")]
pub struct BlockType;

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BlockActivity {
    pub(crate) id: Url,
    #[serde(rename = "type", default)]
    pub(crate) kind: BlockType,
    pub(crate) actor: ObjectId<DbActor>,
    pub(crate) object: Url,
}

#[async_trait::async_trait]
impl Activity for BlockActivity {
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
        let actor_url = self.actor.inner().as_str();
        if let Some(local_user_id) = crate::urls::extract_user_id_from_url(&self.object) {
            let _ = data
                .follow_repo
                .remove_following(local_user_id, actor_url)
                .await;
            let _ = data
                .follow_repo
                .remove_follower(local_user_id, actor_url)
                .await;
            let _ = data
                .blocklist_repo
                .add_blocked_actor(local_user_id, actor_url)
                .await;
        }
        tracing::info!(actor = %actor_url, "received block — removed relationships, recorded in blocklist");
        Ok(())
    }
}
