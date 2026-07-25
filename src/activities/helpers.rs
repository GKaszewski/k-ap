use activitypub_federation::config::Data;
use url::Url;

use crate::data::FederationData;
use crate::error::Error;

/// Returns `true` if the activity was already processed.
/// Marks it processed before returning `false`.
/// On repo error, skips the check rather than silently dropping the activity.
pub(crate) async fn already_processed(activity_id: &Url, data: &Data<FederationData>) -> bool {
    let id = activity_id.as_str();
    match data.activity_repo.is_activity_processed(id).await {
        Ok(true) => {
            tracing::debug!(activity_id = id, "duplicate activity, skipping");
            true
        }
        Ok(false) => {
            if let Err(e) = data.activity_repo.mark_activity_processed(id).await {
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
    let domain = actor.host_str().unwrap_or("");
    tracing::info!(activity_id = %id, source_domain = domain, "processing inbound activity");
    if already_processed(id, data).await {
        return Ok(true);
    }
    if data.blocklist_repo.is_domain_blocked(domain).await? {
        tracing::info!(actor = %actor, "ignoring activity from blocked domain");
        return Ok(true);
    }
    Ok(false)
}

pub(crate) fn verify_attributed_to(
    object: &serde_json::Value,
    actor: &Url,
    activity_name: &str,
) -> Result<(), Error> {
    let attributed_to = object.get("attributedTo").ok_or_else(|| {
        Error::bad_request(format!("{activity_name} object missing attributedTo"))
    })?;

    let actor_urls: Vec<&str> = if let Some(url_str) = attributed_to.as_str() {
        vec![url_str]
    } else if let Some(array) = attributed_to.as_array() {
        array
            .iter()
            .filter_map(|entry| {
                entry
                    .as_str()
                    .or_else(|| entry.get("id").and_then(|id| id.as_str()))
            })
            .collect()
    } else {
        return Err(Error::bad_request(format!(
            "{activity_name} object has invalid attributedTo",
        )));
    };

    let matches_actor = actor_urls
        .iter()
        .any(|url_str| Url::parse(url_str).as_ref() == Ok(actor));

    if !matches_actor {
        return Err(Error::bad_request(format!(
            "{activity_name} actor does not match object attributedTo",
        )));
    }

    Ok(())
}

pub(crate) fn extract_object_ap_id(object: &serde_json::Value, fallback: &Url) -> Url {
    object
        .get("id")
        .and_then(|value| value.as_str())
        .and_then(|id_str| Url::parse(id_str).ok())
        .unwrap_or_else(|| fallback.clone())
}

/// Parse `object["tag"]` for `Mention` entries and notify each tagged local user.
/// Failures are logged and never propagated — a broken mention must not fail the activity.
pub(crate) async fn extract_and_dispatch_mentions(
    ap_id: &Url,
    actor_url: &Url,
    object: &serde_json::Value,
    data: &Data<FederationData>,
) {
    let Some(tags) = object.get("tag").and_then(|tags| tags.as_array()) else {
        return;
    };
    for tag in tags {
        if tag.get("type").and_then(|v| v.as_str()) != Some("Mention") {
            continue;
        }
        let Some(href) = tag.get("href").and_then(|v| v.as_str()) else {
            continue;
        };
        let Ok(href_url) = Url::parse(href) else {
            continue;
        };
        let Some(mentioned_user_id) = data.url_scheme.extract_user_id(&href_url) else {
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
