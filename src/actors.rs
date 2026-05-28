use activitypub_federation::{
    config::Data,
    fetch::object_id::ObjectId,
    http_signatures::generate_actor_keypair,
    protocol::{public_key::PublicKey, verification::verify_domains_match},
    traits::{Actor, Object},
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use url::Url;
use zeroize::Zeroizing;

use crate::data::FederationData;
use crate::error::Error;
use crate::repository::RemoteActor;
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
    pub also_known_as: Option<String>,
    pub profile_url: Option<Url>,
    pub attachment: Vec<ApProfileField>,
    pub manually_approves_followers: bool,
    pub discoverable: bool,
    pub actor_type: ApActorType,
    pub featured_url: Option<Url>,
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
    kind: ApActorType,
    id: ObjectId<DbActor>,
    #[serde(default)]
    preferred_username: String,
    inbox: Url,
    #[serde(default)]
    outbox: Option<Url>,
    #[serde(default)]
    followers: Option<Url>,
    #[serde(default)]
    following: Option<Url>,
    pub public_key: PublicKey,
    #[serde(default)]
    name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    summary: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    icon: Option<ApImageObject>,
    #[serde(skip_serializing_if = "Option::is_none")]
    url: Option<Url>,
    #[serde(skip_serializing_if = "Option::is_none")]
    discoverable: Option<bool>,
    #[serde(default)]
    manually_approves_followers: bool,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    updated: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    endpoints: Option<Endpoints>,
    #[serde(skip_serializing_if = "Option::is_none")]
    image: Option<ApImageObject>,
    #[serde(rename = "alsoKnownAs", skip_serializing_if = "Vec::is_empty", default)]
    also_known_as: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    attachment: Vec<ProfileFieldObject>,
    #[serde(skip_serializing_if = "Option::is_none")]
    featured: Option<Url>,
}

struct ActorUrls {
    ap_id: Url,
    inbox_url: Url,
    shared_inbox_url: Option<Url>,
    outbox_url: Url,
    followers_url: Url,
    following_url: Url,
}

impl ActorUrls {
    fn build(base_url: &str, user_id: uuid::Uuid) -> Self {
        let ap_id = crate::urls::actor_url(base_url, user_id);
        Self {
            inbox_url: Url::parse(&format!("{}/inbox", &ap_id)).expect("valid url"),
            shared_inbox_url: Url::parse(&format!("{}/inbox", base_url)).ok(),
            outbox_url: Url::parse(&format!("{}/outbox", &ap_id)).expect("valid url"),
            followers_url: Url::parse(&format!("{}/followers", &ap_id)).expect("valid url"),
            following_url: Url::parse(&format!("{}/following", &ap_id)).expect("valid url"),
            ap_id,
        }
    }
}

pub async fn get_local_actor(
    user_id: uuid::Uuid,
    data: &Data<FederationData>,
) -> Result<DbActor, Error> {
    let user = data
        .user_repo
        .find_by_id(user_id)
        .await
        .map_err(Error::from)?
        .ok_or_else(|| Error::not_found(anyhow::anyhow!("user not found: {}", user_id)))?;

    let (public_key, private_key) = match data.actor_repo.get_local_actor_keypair(user_id).await? {
        Some(kp) => kp,
        None => {
            let kp = generate_actor_keypair()?;
            // Zeroize the private key after storing it so the plaintext doesn't
            // linger in memory beyond this scope.
            let private_zeroized = Zeroizing::new(kp.private_key.clone());
            data.actor_repo
                .save_local_actor_keypair(
                    user_id,
                    kp.public_key.clone(),
                    private_zeroized.clone().to_string(),
                )
                .await?;
            drop(private_zeroized);
            (kp.public_key, kp.private_key)
        }
    };

    let ActorUrls {
        ap_id,
        inbox_url,
        shared_inbox_url,
        outbox_url,
        followers_url,
        following_url,
    } = ActorUrls::build(&data.base_url, user_id);

    Ok(DbActor {
        user_id,
        username: user.username,
        display_name: user.display_name,
        public_key_pem: public_key,
        private_key_pem: Some(private_key),
        inbox_url,
        shared_inbox_url,
        outbox_url,
        followers_url,
        following_url,
        ap_id,
        last_refreshed_at: Utc::now(),
        bio: user.bio,
        avatar_url: user.avatar_url,
        banner_url: user.banner_url,
        also_known_as: user.also_known_as,
        profile_url: user.profile_url,
        attachment: user.attachment,
        manually_approves_followers: user.manually_approves_followers,
        discoverable: user.discoverable,
        actor_type: user.actor_type,
        featured_url: user.featured_url,
    })
}

fn apex_domain(url: &Url) -> String {
    let host = url.host_str().unwrap_or("");
    host.strip_prefix("www.").unwrap_or(host).to_owned()
}

#[async_trait::async_trait]
impl Object for DbActor {
    type DataType = FederationData;
    type Kind = Person;
    type Error = Error;

    fn id(&self) -> &Url {
        &self.ap_id
    }

    fn last_refreshed_at(&self) -> Option<DateTime<Utc>> {
        Some(self.last_refreshed_at)
    }

    async fn read_from_id(
        object_id: Url,
        data: &Data<Self::DataType>,
    ) -> Result<Option<Self>, Self::Error> {
        let user_id = match crate::urls::extract_user_id_from_url(&object_id) {
            Some(id) => id,
            None => return Ok(None),
        };
        let user = match data.user_repo.find_by_id(user_id).await {
            Ok(Some(u)) => u,
            _ => return Ok(None),
        };

        let keypair = data.actor_repo.get_local_actor_keypair(user_id).await?;

        let (public_key, private_key) = match keypair {
            Some(kp) => (kp.0, Some(kp.1)),
            None => return Ok(None),
        };

        let ActorUrls {
            ap_id,
            inbox_url,
            shared_inbox_url,
            outbox_url,
            followers_url,
            following_url,
        } = ActorUrls::build(&data.base_url, user_id);

        Ok(Some(DbActor {
            user_id,
            username: user.username.clone(),
            display_name: user.display_name,
            public_key_pem: public_key,
            private_key_pem: private_key,
            inbox_url,
            shared_inbox_url,
            outbox_url,
            followers_url,
            following_url,
            ap_id,
            last_refreshed_at: Utc::now(),
            bio: user.bio,
            avatar_url: user.avatar_url,
            banner_url: user.banner_url,
            also_known_as: user.also_known_as,
            profile_url: user.profile_url,
            attachment: user.attachment,
            manually_approves_followers: user.manually_approves_followers,
            discoverable: user.discoverable,
            actor_type: user.actor_type,
            featured_url: user.featured_url,
        }))
    }

    async fn into_json(self, data: &Data<Self::DataType>) -> Result<Self::Kind, Self::Error> {
        let public_key = PublicKey {
            id: format!("{}#main-key", &self.ap_id),
            owner: self.ap_id.clone(),
            public_key_pem: self.public_key_pem.clone(),
        };

        let icon = self.avatar_url.map(|url| ApImageObject {
            kind: "Image".to_string(),
            url,
        });
        let image = self.banner_url.map(|url| ApImageObject {
            kind: "Image".to_string(),
            url,
        });
        let also_known_as: Vec<String> = self.also_known_as.into_iter().collect();
        let attachment: Vec<ProfileFieldObject> = self
            .attachment
            .into_iter()
            .map(|f| ProfileFieldObject {
                kind: "PropertyValue".to_string(),
                name: f.name,
                value: f.value,
            })
            .collect();

        let shared_inbox =
            Url::parse(&format!("{}/inbox", data.base_url)).expect("base_url is always valid");

        Ok(Person {
            kind: self.actor_type,
            id: self.ap_id.clone().into(),
            preferred_username: self.username.clone(),
            inbox: self.inbox_url.clone(),
            outbox: Some(self.outbox_url.clone()),
            followers: Some(self.followers_url.clone()),
            following: Some(self.following_url.clone()),
            public_key,
            name: self.display_name.or_else(|| Some(self.username.clone())),
            summary: self.bio.clone(),
            icon,
            url: self.profile_url,
            discoverable: Some(self.discoverable),
            manually_approves_followers: self.manually_approves_followers,
            updated: Some(self.last_refreshed_at),
            endpoints: Some(Endpoints { shared_inbox }),
            image,
            also_known_as,
            attachment,
            featured: self.featured_url,
        })
    }

    async fn verify(
        json: &Self::Kind,
        expected_domain: &Url,
        _data: &Data<Self::DataType>,
    ) -> Result<(), Self::Error> {
        if verify_domains_match(json.id.inner(), expected_domain).is_ok() {
            return Ok(());
        }
        if apex_domain(json.id.inner()) == apex_domain(expected_domain) {
            tracing::debug!(
                actor_id = %json.id.inner(),
                expected = %expected_domain,
                "domain verified via www-apex equivalence"
            );
            return Ok(());
        }
        verify_domains_match(json.id.inner(), expected_domain).map_err(Error::from)
    }

    async fn from_json(json: Self::Kind, data: &Data<Self::DataType>) -> Result<Self, Self::Error> {
        tracing::debug!(
            actor_id = %json.id.inner(),
            username = %json.preferred_username,
            "ingesting remote actor"
        );
        let shared_inbox_url = json.endpoints.as_ref().map(|e| e.shared_inbox.to_string());
        let actor = RemoteActor {
            url: json.id.inner().to_string(),
            handle: json.preferred_username.clone(),
            inbox_url: json.inbox.to_string(),
            shared_inbox_url,
            display_name: json.name.clone(),
            avatar_url: json.icon.as_ref().map(|i| i.url.to_string()),
            outbox_url: json.outbox.as_ref().map(|u| u.to_string()),
        };
        data.actor_repo.upsert_remote_actor(actor).await?;

        let url_str = json.id.inner().to_string();
        let user_id = uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_URL, url_str.as_bytes());
        let ap_id = json.id.inner().clone();
        let inbox_url = json.inbox.clone();
        let shared_inbox_url = json
            .endpoints
            .as_ref()
            .and_then(|e| Url::parse(e.shared_inbox.as_str()).ok());
        let fallback = |suffix: &str| {
            Url::parse(&format!("{}{}", ap_id, suffix)).unwrap_or_else(|_| ap_id.clone())
        };
        let outbox_url = json.outbox.clone().unwrap_or_else(|| fallback("/outbox"));
        let followers_url = json
            .followers
            .clone()
            .unwrap_or_else(|| fallback("/followers"));
        let following_url = json
            .following
            .clone()
            .unwrap_or_else(|| fallback("/following"));

        Ok(DbActor {
            user_id,
            username: json.preferred_username.clone(),
            display_name: json.name.clone(),
            public_key_pem: json.public_key.public_key_pem,
            private_key_pem: None,
            inbox_url,
            shared_inbox_url,
            outbox_url,
            followers_url,
            following_url,
            ap_id,
            last_refreshed_at: Utc::now(),
            bio: json.summary.clone(),
            avatar_url: json.icon.as_ref().map(|i| i.url.clone()),
            banner_url: json.image.as_ref().map(|i| i.url.clone()),
            also_known_as: json.also_known_as.into_iter().next(),
            profile_url: json.url.clone(),
            attachment: json
                .attachment
                .iter()
                .map(|f| crate::user::ApProfileField {
                    name: f.name.clone(),
                    value: f.value.clone(),
                })
                .collect(),
            manually_approves_followers: json.manually_approves_followers,
            discoverable: json.discoverable.unwrap_or(false),
            actor_type: json.kind,
            featured_url: json.featured,
        })
    }
}

impl Actor for DbActor {
    fn public_key_pem(&self) -> &str {
        &self.public_key_pem
    }

    fn private_key_pem(&self) -> Option<String> {
        self.private_key_pem.clone()
    }

    fn inbox(&self) -> Url {
        self.inbox_url.clone()
    }
}

#[cfg(test)]
#[path = "tests/actors.rs"]
mod tests;
