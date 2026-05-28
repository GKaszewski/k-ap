use std::fmt::Debug;

use activitypub_federation::{activity_sending::SendActivityTask, traits::Activity};
use serde::Serialize;
use url::Url;

use crate::actors::{DbActor, get_local_actor};
use crate::data::{FederationData, FederationEvent};
use crate::error::Error;

use super::ActivityPubService;

pub(crate) async fn send_with_retry(
    sends: Vec<SendActivityTask>,
    data: &activitypub_federation::config::Data<FederationData>,
    max_attempts: u32,
    initial_delay_secs: u64,
) -> Vec<anyhow::Error> {
    let mut failures = vec![];
    for send in sends {
        let mut delay = std::time::Duration::from_secs(initial_delay_secs);
        for attempt in 1..=max_attempts {
            match send.clone().sign_and_send(data).await {
                Ok(()) => break,
                Err(e) if attempt < max_attempts => {
                    tracing::warn!(attempt, error = %e, "delivery failed, retrying");
                    tokio::time::sleep(delay).await;
                    delay *= 2;
                }
                Err(e) => {
                    tracing::error!(attempt, error = %e, "delivery failed permanently");
                    failures.push(anyhow::anyhow!(e));
                }
            }
        }
    }
    failures
}

/// Wraps a pre-serialized AP activity JSON for re-signing via `SendActivityTask::prepare`.
/// Used by `deliver_to_inbox` when a consumer re-presents a persisted queue item.
#[derive(Debug)]
struct RawActivity {
    id: Url,
    actor_url: Url,
    value: serde_json::Value,
}

impl Serialize for RawActivity {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        self.value.serialize(s)
    }
}

#[async_trait::async_trait]
impl Activity for RawActivity {
    type DataType = FederationData;
    type Error = Error;

    fn id(&self) -> &Url { &self.id }
    fn actor(&self) -> &Url { &self.actor_url }

    async fn verify(&self, _data: &activitypub_federation::config::Data<Self::DataType>) -> Result<(), Self::Error> {
        Ok(())
    }
    async fn receive(self, _data: &activitypub_federation::config::Data<Self::DataType>) -> Result<(), Self::Error> {
        Ok(())
    }
}

impl ActivityPubService {
    /// Route deliveries to the EventPublisher (one DeliveryRequested event per inbox)
    /// or fall back to a fire-and-forget tokio::spawn.
    /// `pub(crate)` so sibling modules (broadcast.rs, follow.rs) can call it on `self`.
    pub(crate) async fn dispatch_deliveries(
        &self,
        data: &activitypub_federation::config::Data<FederationData>,
        local_actor: &DbActor,
        inboxes: Vec<Url>,
        sends: Vec<SendActivityTask>,
        activity_json: serde_json::Value,
    ) -> anyhow::Result<()> {
        if let Some(publisher) = data.event_publisher.as_ref() {
            for inbox in inboxes {
                let event = FederationEvent::DeliveryRequested {
                    inbox,
                    activity: activity_json.clone(),
                    signing_actor_id: local_actor.user_id,
                };
                if let Err(e) = publisher.publish(event).await {
                    tracing::warn!(error = %e, "failed to enqueue DeliveryRequested event");
                }
            }
        } else {
            let data = data.clone();
            let max_attempts = self.delivery_max_attempts;
            let initial_delay = self.delivery_initial_delay_secs;
            tokio::spawn(async move {
                let failures =
                    send_with_retry(sends, &data, max_attempts, initial_delay).await;
                if !failures.is_empty() {
                    tracing::warn!(count = failures.len(), "some deliveries failed permanently");
                }
            });
        }
        Ok(())
    }

    /// Deliver a single outbound activity to `inbox`.
    /// Call from a job-queue consumer processing a `FederationEvent::DeliveryRequested` event.
    pub async fn deliver_to_inbox(
        &self,
        inbox: url::Url,
        activity: serde_json::Value,
        signing_actor_id: uuid::Uuid,
    ) -> anyhow::Result<()> {
        let data = self.federation_config.to_request_data();
        let actor = get_local_actor(signing_actor_id, &data)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let id = activity
            .get("id")
            .and_then(|v| v.as_str())
            .and_then(|s| Url::parse(s).ok())
            .unwrap_or_else(|| actor.ap_id.clone());
        let actor_url = activity
            .get("actor")
            .and_then(|v| v.as_str())
            .and_then(|s| Url::parse(s).ok())
            .unwrap_or_else(|| actor.ap_id.clone());
        let raw = RawActivity { id, actor_url, value: activity.clone() };
        let sends =
            SendActivityTask::prepare(&raw, &actor, vec![inbox.clone()], &data).await?;
        let failures = send_with_retry(
            sends,
            &data,
            self.delivery_max_attempts,
            self.delivery_initial_delay_secs,
        )
        .await;
        if failures.is_empty() {
            return Ok(());
        }
        let error_msg = failures
            .iter()
            .map(|e| e.to_string())
            .collect::<Vec<_>>()
            .join("; ");
        if let Some(publisher) = data.event_publisher.as_ref() {
            let _ = publisher
                .publish(FederationEvent::DeliveryFailed {
                    inbox,
                    activity,
                    signing_actor_id,
                    error: error_msg.clone(),
                })
                .await;
        }
        Err(anyhow::anyhow!("delivery failed: {}", error_msg))
    }

    /// Serialize `activity` to JSON and prepare `SendActivityTask` objects.
    /// Returns `(activity_json, sends, inboxes)` so both dispatch paths have what they need.
    /// `pub(super)` — visible to all child modules of `service` (broadcast.rs, follow.rs, etc.).
    pub(super) async fn prepare_broadcast<A>(
        &self,
        data: &activitypub_federation::config::Data<FederationData>,
        local_actor: &DbActor,
        inboxes: Vec<Url>,
        activity: A,
    ) -> anyhow::Result<(serde_json::Value, Vec<SendActivityTask>, Vec<Url>)>
    where
        A: Activity + Serialize + Debug + Send + Sync,
    {
        let with_ctx = activitypub_federation::protocol::context::WithContext::new_default(activity);
        let activity_json = serde_json::to_value(&with_ctx)?;
        let sends =
            SendActivityTask::prepare(&with_ctx, local_actor, inboxes.clone(), data).await?;
        Ok((activity_json, sends, inboxes))
    }
}
