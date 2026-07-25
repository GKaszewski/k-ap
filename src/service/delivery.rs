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
    tracing::info!(
        inbox_count = sends.len(),
        max_attempts,
        "starting outbound delivery"
    );
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

    fn id(&self) -> &Url {
        &self.id
    }
    fn actor(&self) -> &Url {
        &self.actor_url
    }

    async fn verify(
        &self,
        _data: &activitypub_federation::config::Data<Self::DataType>,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
    async fn receive(
        self,
        _data: &activitypub_federation::config::Data<Self::DataType>,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

impl ActivityPubService {
    /// Dispatch pre-built `SendActivityTask`s via the event publisher (if configured)
    /// or by spawning a background retry loop.
    fn dispatch_sends(
        &self,
        data: &activitypub_federation::config::Data<FederationData>,
        local_actor: &DbActor,
        inboxes: Vec<Url>,
        sends: Vec<SendActivityTask>,
        activity_json: serde_json::Value,
    ) {
        if let Some(publisher) = data.event_publisher.as_ref() {
            let publisher = publisher.clone();
            let signing_actor_id = local_actor.user_id;
            let activity = activity_json;
            tokio::spawn(async move {
                for inbox in inboxes {
                    let event = FederationEvent::DeliveryRequested {
                        inbox,
                        activity: activity.clone(),
                        signing_actor_id,
                    };
                    if let Err(error) = publisher.publish(event).await {
                        tracing::warn!(%error, "failed to enqueue DeliveryRequested event");
                    }
                }
            });
        } else {
            let data = data.clone();
            let max_attempts = self.delivery_max_attempts;
            let initial_delay = self.delivery_initial_delay_secs;
            tokio::spawn(async move {
                let failures = send_with_retry(sends, &data, max_attempts, initial_delay).await;
                if !failures.is_empty() {
                    tracing::warn!(count = failures.len(), "some deliveries failed permanently");
                }
            });
        }
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
        let actor = get_local_actor(signing_actor_id, &data).await?;

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
        let raw = RawActivity {
            id,
            actor_url,
            value: activity.clone(),
        };

        let sends = SendActivityTask::prepare(&raw, &actor, vec![inbox.clone()], &data).await?;
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

        if let Some(publisher) = data.event_publisher.as_ref()
            && let Err(error) = publisher
                .publish(FederationEvent::DeliveryFailed {
                    inbox,
                    activity,
                    signing_actor_id,
                    error: error_msg.clone(),
                })
                .await
        {
            tracing::warn!(%error, "failed to publish DeliveryFailed event");
        }
        Err(anyhow::anyhow!("delivery failed: {}", error_msg))
    }

    pub(super) async fn send_activity<A>(
        &self,
        data: &activitypub_federation::config::Data<FederationData>,
        local_actor: &DbActor,
        inboxes: Vec<Url>,
        activity: A,
    ) -> anyhow::Result<()>
    where
        A: Activity + Serialize + Debug + Send + Sync,
    {
        let with_ctx =
            activitypub_federation::protocol::context::WithContext::new_default(activity);
        let activity_json = serde_json::to_value(&with_ctx)?;
        let sends =
            SendActivityTask::prepare(&with_ctx, local_actor, inboxes.clone(), data).await?;
        self.dispatch_sends(data, local_actor, inboxes, sends, activity_json);
        Ok(())
    }

    pub(super) async fn send_raw_activity(
        &self,
        data: &activitypub_federation::config::Data<FederationData>,
        local_actor: &DbActor,
        inboxes: Vec<Url>,
        activity: serde_json::Value,
    ) -> anyhow::Result<()> {
        let id = activity
            .get("id")
            .and_then(|value| value.as_str())
            .and_then(|id_str| Url::parse(id_str).ok())
            .unwrap_or_else(|| local_actor.ap_id.clone());
        let actor_url = activity
            .get("actor")
            .and_then(|value| value.as_str())
            .and_then(|actor_str| Url::parse(actor_str).ok())
            .unwrap_or_else(|| local_actor.ap_id.clone());

        let raw = RawActivity {
            id,
            actor_url,
            value: activity.clone(),
        };
        let sends = SendActivityTask::prepare(&raw, local_actor, inboxes.clone(), data).await?;
        self.dispatch_sends(data, local_actor, inboxes, sends, activity);
        Ok(())
    }
}
