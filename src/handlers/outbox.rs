use axum::extract::{Path, Query};
use axum::response::IntoResponse;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use url::Url;

use activitypub_federation::{
    config::Data, fetch::object_id::ObjectId, kinds::activity::CreateType,
    protocol::context::WithContext,
};

use crate::{
    activities::CreateActivity, content::LocalObject, data::FederationData, error::Error,
    urls::AP_PAGE_SIZE,
};

#[derive(Deserialize)]
pub struct OutboxQuery {
    page: Option<bool>,
    before: Option<String>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderedCollection {
    #[serde(rename = "@context")]
    context: String,
    #[serde(rename = "type")]
    kind: String,
    id: String,
    total_items: u64,
    first: String,
    last: String,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderedCollectionPage {
    #[serde(rename = "@context")]
    context: String,
    #[serde(rename = "type")]
    kind: String,
    id: String,
    part_of: String,
    total_items: u64,
    ordered_items: Vec<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    next: Option<String>,
}

pub async fn outbox_handler(
    Path(user_id_str): Path<String>,
    Query(query): Query<OutboxQuery>,
    data: Data<FederationData>,
) -> Result<axum::response::Response, Error> {
    let uuid =
        uuid::Uuid::parse_str(&user_id_str).map_err(|_| Error::bad_request("invalid user id"))?;

    data.user_repo
        .find_by_id(uuid)
        .await?
        .ok_or_else(|| Error::not_found("user not found"))?;

    let actor_url = data.url_scheme.actor_url(&data.base_url, uuid)?;
    let outbox_url = data.url_scheme.outbox_url(&actor_url)?.to_string();
    let total = data.content_reader.count_local_posts().await?;

    if query.page.unwrap_or(false) {
        build_outbox_page(uuid, &query, &outbox_url, total, &data).await
    } else {
        build_outbox_collection(&outbox_url, total)
    }
}

async fn build_outbox_page(
    user_id: uuid::Uuid,
    query: &OutboxQuery,
    outbox_url: &str,
    total: u64,
    data: &Data<FederationData>,
) -> Result<axum::response::Response, Error> {
    let before: Option<DateTime<Utc>> = query.before.as_deref().and_then(|s| s.parse().ok());
    let items = data
        .content_reader
        .get_local_objects_page(user_id, before, AP_PAGE_SIZE)
        .await?;

    let actor_url: Url = data
        .url_scheme
        .actor_url(&data.base_url, user_id)
        .map_err(|error| Error::bad_request(format!("invalid base_url: {error}")))?;

    let has_more = items.len() == AP_PAGE_SIZE;
    let oldest_timestamp = items.last().map(|item| item.published_at);
    let ordered_items = wrap_items_as_create_activities(&items, &actor_url)?;

    let page_id = match &query.before {
        Some(before) => format!("{}?page=true&before={}", outbox_url, before),
        None => format!("{}?page=true", outbox_url),
    };

    let next = if has_more {
        oldest_timestamp.map(|timestamp| {
            let formatted = timestamp.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string();
            format!("{}?page=true&before={}", outbox_url, formatted)
        })
    } else {
        None
    };

    Ok(axum::Json(OrderedCollectionPage {
        context: crate::urls::AP_CONTEXT.to_string(),
        kind: "OrderedCollectionPage".to_string(),
        id: page_id,
        part_of: outbox_url.to_string(),
        total_items: total,
        ordered_items,
        next,
    })
    .into_response())
}

fn build_outbox_collection(
    outbox_url: &str,
    total: u64,
) -> Result<axum::response::Response, Error> {
    Ok(axum::Json(OrderedCollection {
        context: crate::urls::AP_CONTEXT.to_string(),
        kind: "OrderedCollection".to_string(),
        id: outbox_url.to_string(),
        total_items: total,
        first: format!("{}?page=true", outbox_url),
        last: format!("{}?page=true&before=1970-01-01T00:00:00.000Z", outbox_url),
    })
    .into_response())
}

fn wrap_items_as_create_activities(
    items: &[LocalObject],
    actor_url: &Url,
) -> Result<Vec<serde_json::Value>, Error> {
    items
        .iter()
        .map(|item| {
            let create_id = Url::parse(&format!("{}/activity", item.ap_id))
                .map_err(|error| anyhow::anyhow!(error))?;

            let activity = WithContext::new_default(CreateActivity {
                id: create_id,
                kind: CreateType::default(),
                actor: ObjectId::from(actor_url.clone()),
                object: item.object.clone(),
                to: item.to.clone(),
                cc: item.cc.clone(),
                bto: vec![],
                bcc: vec![],
            });

            serde_json::to_value(activity).map_err(|error| anyhow::anyhow!(error).into())
        })
        .collect()
}
