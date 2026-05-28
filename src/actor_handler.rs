use activitypub_federation::{
    axum::json::FederationJson, config::Data, protocol::context::WithContext, traits::Object,
};
use axum::extract::Path;

use crate::actors::{Person, get_local_actor};
use crate::data::FederationData;
use crate::error::Error;

/// Serves the AP actor JSON for a local user.
/// The path parameter is the user's UUID (matching the canonical actor URL).
pub async fn actor_handler(
    Path(user_id_str): Path<String>,
    data: Data<FederationData>,
) -> Result<FederationJson<WithContext<Person>>, Error> {
    let user_id = uuid::Uuid::parse_str(&user_id_str)
        .map_err(|_| Error::not_found(anyhow::anyhow!("user not found")))?;

    let db_actor = get_local_actor(user_id, &data).await?;
    let person = db_actor.into_json(&data).await?;

    Ok(FederationJson(WithContext::new_default(person)))
}
