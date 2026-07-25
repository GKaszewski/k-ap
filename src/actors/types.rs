use activitypub_federation::fetch::object_id::ObjectId;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use url::Url;

use crate::url_scheme::UrlScheme;
use crate::user::{ApActorType, ApProfileField};

#[derive(Debug, Clone)]
pub struct DbActor {
    pub user_id: uuid::Uuid,
    pub username: String,
    pub display_name: Option<String>,
    pub public_key_pem: String,
    /// Private key PEM. Only populated for local actors during signing.
    /// Cleared automatically when `DbActor` is dropped.
    pub private_key_pem: Option<String>,
    pub inbox_url: Url,
    pub shared_inbox_url: Option<Url>,
    pub outbox_url: Url,
    pub followers_url: Url,
    pub following_url: Url,
    pub ap_id: Url,
    pub last_refreshed_at: DateTime<Utc>,
    pub bio: Option<String>,
    pub avatar_url: Option<Url>,
    pub banner_url: Option<Url>,
    pub also_known_as: Vec<String>,
    pub profile_url: Option<Url>,
    pub attachment: Vec<ApProfileField>,
    pub manually_approves_followers: bool,
    pub discoverable: bool,
    pub actor_type: ApActorType,
    pub featured_url: Option<Url>,
}

impl DbActor {
    pub fn object_id(&self) -> ObjectId<Self> {
        ObjectId::from(self.ap_id.clone())
    }
}

pub(super) struct ActorUrls {
    pub(super) ap_id: Url,
    pub(super) inbox_url: Url,
    pub(super) shared_inbox_url: Option<Url>,
    pub(super) outbox_url: Url,
    pub(super) followers_url: Url,
    pub(super) following_url: Url,
}

impl ActorUrls {
    pub(super) fn build(
        base_url: &str,
        user_id: uuid::Uuid,
        url_scheme: &dyn UrlScheme,
    ) -> anyhow::Result<Self> {
        let ap_id = url_scheme.actor_url(base_url, user_id)?;
        Ok(Self {
            inbox_url: url_scheme.inbox_url(&ap_id)?,
            shared_inbox_url: url_scheme.shared_inbox_url(base_url),
            outbox_url: url_scheme.outbox_url(&ap_id)?,
            followers_url: url_scheme.followers_url(&ap_id)?,
            following_url: url_scheme.following_url(&ap_id)?,
            ap_id,
        })
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ApImageObject {
    #[serde(rename = "type")]
    pub kind: String,
    pub url: Url,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Endpoints {
    pub shared_inbox: Url,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProfileFieldObject {
    #[serde(rename = "type")]
    pub kind: String,
    pub name: String,
    pub value: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Person {
    #[serde(rename = "type")]
    pub(crate) kind: ApActorType,
    pub(crate) id: ObjectId<DbActor>,
    #[serde(default)]
    pub(crate) preferred_username: String,
    pub(crate) inbox: Url,
    #[serde(default)]
    pub(crate) outbox: Option<Url>,
    #[serde(default)]
    pub(crate) followers: Option<Url>,
    #[serde(default)]
    pub(crate) following: Option<Url>,
    pub public_key: activitypub_federation::protocol::public_key::PublicKey,
    #[serde(default)]
    pub(crate) name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) summary: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) icon: Option<ApImageObject>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) url: Option<Url>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) discoverable: Option<bool>,
    #[serde(default)]
    pub(crate) manually_approves_followers: bool,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub(crate) updated: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) endpoints: Option<Endpoints>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) image: Option<ApImageObject>,
    #[serde(rename = "alsoKnownAs", skip_serializing_if = "Vec::is_empty", default)]
    pub(crate) also_known_as: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub(crate) attachment: Vec<ProfileFieldObject>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) featured: Option<Url>,
}
