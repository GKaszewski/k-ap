use activitypub_federation::{
    config::Data, fetch::object_id::ObjectId, kinds::activity::FollowType, traits::Activity,
};
use serde::{Deserialize, Serialize};
use url::Url;

use crate::actors::DbActor;
use crate::data::FederationData;
use crate::error::Error;
use crate::repository::FollowerStatus;

use super::helpers::check_guards;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FollowActivity {
    pub(crate) id: Url,
    #[serde(rename = "type", default)]
    pub(crate) kind: FollowType,
    pub(crate) actor: ObjectId<DbActor>,
    pub(crate) object: ObjectId<DbActor>,
}

#[async_trait::async_trait]
impl Activity for FollowActivity {
    type DataType = FederationData;
    type Error = Error;

    fn id(&self) -> &Url {
        &self.id
    }
    fn actor(&self) -> &Url {
        self.actor.inner()
    }

    async fn verify(&self, data: &Data<Self::DataType>) -> Result<(), Self::Error> {
        let target_url = self.object.inner();
        let target_domain = match (target_url.host_str(), target_url.port()) {
            (Some(host), Some(port)) => format!("{}:{}", host, port),
            (Some(host), None) => host.to_string(),
            _ => {
                return Err(Error::bad_request(anyhow::anyhow!(
                    "invalid follow target URL"
                )));
            }
        };
        if target_domain == data.domain {
            return Ok(());
        }
        if let Some(uuid) = crate::urls::extract_user_id_from_url(target_url)
            && data
                .user_repo
                .find_by_id(uuid)
                .await
                .ok()
                .flatten()
                .is_some()
            {
                tracing::debug!(target = %target_url, "accepting follow for migrated actor URL");
                return Ok(());
            }
        Err(Error::bad_request(anyhow::anyhow!(
            "follow target is not a local actor"
        )))
    }

    async fn receive(self, data: &Data<Self::DataType>) -> Result<(), Self::Error> {
        if check_guards(&self.id, self.actor.inner(), data).await? {
            return Ok(());
        }
        // Actor block checked BEFORE any outbound HTTP fetch.
        if let Some(target_user_id) = crate::urls::extract_user_id_from_url(self.object.inner())
            && data
                .blocklist_repo
                .is_actor_blocked(target_user_id, self.actor.inner().as_str())
                .await?
            {
                tracing::info!(actor = %self.actor.inner(), "ignoring follow from blocked actor");
                return Ok(());
            }
        let _follower = self.actor.dereference(data).await?;
        let local_actor = self.object.dereference(data).await?;
        data.follow_repo
            .add_follower(
                local_actor.user_id,
                self.actor.inner().as_str(),
                FollowerStatus::Pending,
                self.id.as_str(),
            )
            .await?;
        tracing::info!(
            follower = %self.actor.inner(),
            local_user = %local_actor.user_id,
            "follow request pending approval"
        );
        Ok(())
    }
}
