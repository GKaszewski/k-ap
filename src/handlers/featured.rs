use activitypub_federation::{axum::json::FederationJson, config::Data};
use axum::extract::Path;
use serde_json::json;

use crate::data::FederationData;
use crate::error::Error;
use crate::urls::AP_CONTEXT;

/// Serves the `featured` (pinned posts) `OrderedCollection` for a local user.
///
/// Remote servers follow the `featured` link from the actor JSON and expect
/// an `OrderedCollection` whose `orderedItems` are the AP URLs of pinned objects.
/// The handler calls [`ApContentReader::get_featured_objects`] — override that
/// method to expose your pinned posts.
pub async fn featured_handler(
    Path(user_id_str): Path<String>,
    data: Data<FederationData>,
) -> Result<FederationJson<serde_json::Value>, Error> {
    let user_id =
        uuid::Uuid::parse_str(&user_id_str).map_err(|_| Error::not_found("user not found"))?;

    data.user_repo
        .find_by_id(user_id)
        .await?
        .ok_or_else(|| Error::not_found("user not found"))?;

    let featured_url = format!("{}/users/{}/featured", data.base_url, user_id_str);
    let items = data.content_reader.get_featured_objects(user_id).await?;

    Ok(FederationJson(json!({
        "@context": AP_CONTEXT,
        "type": "OrderedCollection",
        "id": featured_url,
        "totalItems": items.len(),
        "orderedItems": items.iter().map(|url| url.as_str()).collect::<Vec<_>>(),
    })))
}
