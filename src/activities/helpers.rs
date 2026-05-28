use activitypub_federation::config::Data;
use url::Url;

use crate::data::FederationData;
use crate::error::Error;

/// Returns `true` if the activity was already processed.
/// Marks it processed before returning `false`.
/// On repo error, skips the check rather than silently dropping the activity.
pub(crate) async fn already_processed(activity_id: &Url, data: &Data<FederationData>) -> bool {
    let id = activity_id.as_str();
    match data.federation_repo.is_activity_processed(id).await {
        Ok(true) => {
            tracing::debug!(activity_id = id, "duplicate activity, skipping");
            true
        }
        Ok(false) => {
            if let Err(e) = data.federation_repo.mark_activity_processed(id).await {
                tracing::warn!(activity_id = id, error = %e, "failed to mark activity processed");
            }
            false
        }
        Err(e) => {
            tracing::warn!(error = %e, "idempotency check failed, processing anyway");
            false
        }
    }
}

/// Returns `true` when the activity should be skipped:
/// already processed, or the sender's domain is blocked.
/// Call this at the top of every `receive()` impl.
pub(crate) async fn check_guards(
    id: &Url,
    actor: &Url,
    data: &Data<FederationData>,
) -> Result<bool, Error> {
    if already_processed(id, data).await {
        return Ok(true);
    }
    let domain = actor.host_str().unwrap_or("");
    if data.federation_repo.is_domain_blocked(domain).await? {
        tracing::info!(actor = %actor, "ignoring activity from blocked domain");
        return Ok(true);
    }
    Ok(false)
}

/// Parse `object["tag"]` for `Mention` entries and notify each tagged local user.
/// Failures are logged and never propagated — a broken mention must not fail the activity.
pub(crate) async fn extract_and_dispatch_mentions(
    ap_id: &Url,
    actor_url: &Url,
    object: &serde_json::Value,
    data: &Data<FederationData>,
) {
    let Some(tags) = object.get("tag").and_then(|t| t.as_array()) else {
        return;
    };
    for tag in tags {
        if tag.get("type").and_then(|v| v.as_str()) != Some("Mention") {
            continue;
        }
        let Some(href) = tag.get("href").and_then(|v| v.as_str()) else {
            continue;
        };
        let Ok(href_url) = Url::parse(href) else { continue };
        let Some(mentioned_user_id) = crate::urls::extract_user_id_from_url(&href_url) else {
            continue;
        };
        if let Err(e) = data
            .object_handler
            .on_mention(ap_id, mentioned_user_id, actor_url)
            .await
        {
            tracing::warn!(
                ap_id = %ap_id,
                mentioned_user = %mentioned_user_id,
                error = %e,
                "failed to dispatch mention notification"
            );
        }
    }
}
