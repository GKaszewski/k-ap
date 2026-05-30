use activitypub_federation::{
    activity_sending::SendActivityTask, fetch::object_id::ObjectId, protocol::context::WithContext,
};
use url::Url;

use crate::{activities::CreateActivity, actors::get_local_actor, federation::ApFederationConfig};

use super::{ActivityPubService, delivery::send_with_retry};

impl ActivityPubService {
    /// Fetch a remote actor's outbox and import its content into the local instance.
    ///
    /// This is for importing a **remote actor's history** — for example, when you want
    /// to surface an account's past posts after a local user follows them. It fetches
    /// pages from `outbox_url` and calls `ApObjectHandler::on_create` for each item.
    ///
    /// This is distinct from [`ActivityPubService::run_backfill_for_follower`], which
    /// sends **your** local content to a new follower's inbox.
    pub async fn import_remote_outbox(
        &self,
        outbox_url: &str,
        actor_url: &str,
    ) -> anyhow::Result<()> {
        let outbox_parsed = url::Url::parse(outbox_url)?;
        crate::security::validate_url(&outbox_parsed).await?;
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(
                super::HTTP_FETCH_TIMEOUT_SECS,
            ))
            .build()?;
        let data = self.federation_config.to_request_data();
        let actor = url::Url::parse(actor_url)?;
        let root: serde_json::Value = client
            .get(outbox_url)
            .header("Accept", "application/activity+json")
            .send()
            .await?
            .json()
            .await?;
        let first = match root.get("first").and_then(|v| v.as_str()) {
            Some(url) => url.to_string(),
            None => {
                tracing::debug!(outbox = %outbox_url, "outbox has no first page");
                return Ok(());
            }
        };
        let mut current_url = first;
        let mut visited = std::collections::HashSet::new();
        loop {
            if !visited.insert(current_url.clone()) {
                tracing::warn!(url = %current_url, "backfill: loop detected, stopping");
                break;
            }
            if let Ok(page_url) = url::Url::parse(&current_url)
                && let Err(e) = crate::security::validate_url(&page_url).await
            {
                tracing::warn!(url = %current_url, error = %e, "backfill: SSRF check failed");
                break;
            }
            let page: serde_json::Value = match client
                .get(&current_url)
                .header("Accept", "application/activity+json")
                .send()
                .await
            {
                Ok(resp) => match resp.json().await {
                    Ok(v) => v,
                    Err(e) => {
                        tracing::error!(error = %e, "backfill: failed to parse page JSON");
                        break;
                    }
                },
                Err(e) => {
                    tracing::error!(error = %e, "backfill: HTTP request failed");
                    break;
                }
            };
            if let Some(items) = page.get("orderedItems").and_then(|v| v.as_array()) {
                for item in items {
                    let activity_type = item.get("type").and_then(|v| v.as_str()).unwrap_or("");
                    if activity_type != "Create" && activity_type != "Add" {
                        continue;
                    }
                    let Some(object) = item.get("object").filter(|o| o.is_object()).cloned() else {
                        continue;
                    };
                    let Some(ap_id) = object
                        .get("id")
                        .and_then(|v| v.as_str())
                        .and_then(|s| url::Url::parse(s).ok())
                    else {
                        continue;
                    };
                    if let Err(e) = data.object_handler.on_create(&ap_id, &actor, object).await {
                        tracing::warn!(ap_id = %ap_id, error = %e, "backfill: failed to process item");
                    }
                }
            }
            match page.get("next").and_then(|v| v.as_str()) {
                Some(next) => current_url = next.to_string(),
                None => break,
            }
        }
        tracing::info!(outbox = %outbox_url, pages = visited.len(), "backfill complete");
        Ok(())
    }

    /// Route backfill through [`EventPublisher`] (if configured) or fall back
    /// to a fire-and-forget `tokio::spawn`.
    ///
    /// When `EventPublisher` is set, a [`FederationEvent::BackfillRequested`]
    /// event is published so the consumer's job queue can process it — allowing
    /// backfill to run in a separate worker process rather than in the API server.
    /// The worker calls [`ActivityPubService::run_backfill_for_follower`] to execute.
    ///
    /// `pub(crate)` so `service::follow` can call it from `accept_follower`.
    pub(crate) fn spawn_backfill(&self, owner_user_id: uuid::Uuid, follower_inbox_url: String) {
        let data = self.federation_config.to_request_data();
        if let Some(publisher) = data.event_publisher.as_ref() {
            let publisher = publisher.clone();
            let event = crate::data::FederationEvent::BackfillRequested {
                owner_user_id,
                follower_inbox_url,
            };
            tokio::spawn(async move {
                if let Err(e) = publisher.publish(event).await {
                    tracing::warn!(error = %e, "failed to enqueue BackfillRequested event");
                }
            });
        } else {
            let config = self.federation_config.clone();
            let base_url = self.base_url.clone();
            let max_attempts = self.delivery_max_attempts;
            let initial_delay = self.delivery_initial_delay_secs;
            tokio::spawn(async move {
                if let Err(e) = ActivityPubService::run_backfill(
                    config,
                    base_url,
                    owner_user_id,
                    follower_inbox_url,
                    max_attempts,
                    initial_delay,
                )
                .await
                {
                    tracing::warn!(error = %e, "backfill: task failed");
                }
            });
        }
    }

    /// Execute backfill for a single follower inbox. Call this from a job-queue
    /// consumer that received a [`FederationEvent::BackfillRequested`] event.
    ///
    /// Sends all of `owner_user_id`'s locally-authored content to `follower_inbox_url`,
    /// oldest-to-newest, with a small sleep between batches to avoid overwhelming
    /// the remote server.
    pub async fn run_backfill_for_follower(
        &self,
        owner_user_id: uuid::Uuid,
        follower_inbox_url: String,
    ) -> anyhow::Result<()> {
        ActivityPubService::run_backfill(
            self.federation_config.clone(),
            self.base_url.clone(),
            owner_user_id,
            follower_inbox_url,
            self.delivery_max_attempts,
            self.delivery_initial_delay_secs,
        )
        .await
    }

    async fn run_backfill(
        config: ApFederationConfig,
        base_url: String,
        owner_user_id: uuid::Uuid,
        follower_inbox_url: String,
        max_attempts: u32,
        initial_delay: u64,
    ) -> anyhow::Result<()> {
        const BATCH_SIZE: usize = 20;
        let data = config.to_request_data();
        let local_actor = get_local_actor(owner_user_id, &data)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let inbox = Url::parse(&follower_inbox_url)?;

        // Cursor-based pagination via get_local_objects_page (newest-first).
        // Avoids loading the entire post history into memory at once.
        let mut before: Option<chrono::DateTime<chrono::Utc>> = None;
        let (mut success_count, mut failure_count, mut total) = (0usize, 0usize, 0usize);

        loop {
            let page = data
                .content_reader
                .get_local_objects_page(owner_user_id, before, BATCH_SIZE)
                .await?;

            if page.is_empty() {
                break;
            }

            let is_last_page = page.len() < BATCH_SIZE;
            // Advance cursor to the oldest timestamp in this page.
            before = page.last().map(|(_, _, ts)| *ts);

            for (ap_id, object_json, _ts) in &page {
                let create_id = Url::parse(&format!(
                    "{}/activities/create/{}",
                    base_url,
                    uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_URL, ap_id.as_str().as_bytes())
                ))?;
                let create = CreateActivity {
                    id: create_id,
                    kind: Default::default(),
                    actor: ObjectId::from(local_actor.ap_id.clone()),
                    object: object_json.clone(),
                    to: vec![],
                    cc: vec![],
                    bto: vec![],
                    bcc: vec![],
                };
                let sends = SendActivityTask::prepare(
                    &WithContext::new_default(create),
                    &local_actor,
                    vec![inbox.clone()],
                    &data,
                )
                .await?;
                total += 1;
                if send_with_retry(sends, &data, max_attempts, initial_delay)
                    .await
                    .is_empty()
                {
                    success_count += 1;
                } else {
                    failure_count += 1;
                }
            }

            if is_last_page {
                break;
            }

            tokio::time::sleep(std::time::Duration::from_millis(
                super::BATCH_FETCH_SLEEP_MS,
            ))
            .await;
        }

        tracing::info!(
            user_id = %owner_user_id,
            follower = %follower_inbox_url,
            sent = success_count,
            failed = failure_count,
            total = total,
            "backfill complete"
        );
        Ok(())
    }
}
