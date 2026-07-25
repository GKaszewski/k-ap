use activitypub_federation::{config::Data, fetch::webfinger::extract_webfinger_name};
use axum::{
    extract::Query,
    http::header,
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};

use crate::data::FederationData;
use crate::error::Error;

#[derive(Deserialize)]
pub struct WebfingerQuery {
    resource: String,
}

#[derive(Serialize)]
struct WebfingerLink {
    rel: String,
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    href: Option<String>,
}

#[derive(Serialize)]
struct WebfingerResponse {
    subject: String,
    /// Canonical URIs for the same account (acct: URI + AP actor URL).
    aliases: Vec<String>,
    links: Vec<WebfingerLink>,
}

pub async fn webfinger_handler(
    Query(query): Query<WebfingerQuery>,
    data: Data<FederationData>,
) -> Result<Response, Error> {
    let name = extract_webfinger_name(&query.resource, &data)?;

    let user = data
        .user_repo
        .find_by_username(name)
        .await?
        .ok_or_else(|| Error::not_found("user not found"))?;

    let ap_id = data.url_scheme.actor_url(&data.base_url, user.id)?;
    let acct_uri = format!("acct:{}@{}", user.username, data.domain);

    let response = WebfingerResponse {
        subject: query.resource.clone(),
        aliases: vec![acct_uri, ap_id.to_string()],
        links: vec![
            WebfingerLink {
                rel: "http://webfinger.net/rel/profile-page".to_string(),
                kind: Some("text/html".to_string()),
                href: Some(ap_id.to_string()),
            },
            WebfingerLink {
                rel: "self".to_string(),
                kind: Some(crate::urls::AP_CONTENT_TYPE.to_string()),
                href: Some(ap_id.to_string()),
            },
        ],
    };

    let body = serde_json::to_string(&response).map_err(|error| anyhow::anyhow!(error))?;
    Ok(([(header::CONTENT_TYPE, "application/jrd+json")], body).into_response())
}
