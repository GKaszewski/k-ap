use activitypub_federation::{axum::json::FederationJson, config::Data};
use axum::extract::{Path, Query};
use serde::Deserialize;

use crate::data::FederationData;
use crate::error::Error;
use crate::service::collections::serialize_ordered_collection;

#[derive(Deserialize)]
pub struct PageQuery {
    page: Option<u32>,
}

async fn collection_handler(
    user_id_str: &str,
    query: PageQuery,
    data: Data<FederationData>,
    collection_type: &str,
) -> Result<FederationJson<serde_json::Value>, Error> {
    let user_id =
        uuid::Uuid::parse_str(user_id_str).map_err(|_| Error::bad_request("invalid user id"))?;

    data.user_repo
        .find_by_id(user_id)
        .await?
        .ok_or_else(|| Error::not_found("user not found"))?;

    let actor_url = data
        .url_scheme
        .actor_url(&data.base_url, user_id)
        .map_err(Error::from)?;
    let collection_url = match collection_type {
        "followers" => data.url_scheme.followers_url(&actor_url),
        _ => data.url_scheme.following_url(&actor_url),
    }
    .map_err(Error::from)?
    .to_string();

    let total = match collection_type {
        "followers" => data.follow_repo.count_followers(user_id).await,
        _ => data.follow_repo.count_following(user_id).await,
    }
    .map_err(Error::from)?;

    let items_fn = |offset: u32, limit: usize| {
        let data = data.clone();
        async move {
            Ok(match collection_type {
                "followers" => data
                    .follow_repo
                    .get_followers_page(user_id, offset, limit)
                    .await?
                    .into_iter()
                    .map(|follower| follower.actor.url)
                    .collect(),
                _ => data
                    .follow_repo
                    .get_following_page(user_id, offset, limit)
                    .await?
                    .into_iter()
                    .map(|actor| actor.url)
                    .collect(),
            })
        }
    };

    let json_str = serialize_ordered_collection(&collection_url, total, query.page, items_fn)
        .await
        .map_err(Error::from)?;
    let value: serde_json::Value =
        serde_json::from_str(&json_str).map_err(|e| Error::from(anyhow::anyhow!(e)))?;
    Ok(FederationJson(value))
}

pub async fn followers_handler(
    Path(user_id_str): Path<String>,
    Query(query): Query<PageQuery>,
    data: Data<FederationData>,
) -> Result<FederationJson<serde_json::Value>, Error> {
    collection_handler(&user_id_str, query, data, "followers").await
}

pub async fn following_handler(
    Path(user_id_str): Path<String>,
    Query(query): Query<PageQuery>,
    data: Data<FederationData>,
) -> Result<FederationJson<serde_json::Value>, Error> {
    collection_handler(&user_id_str, query, data, "following").await
}
