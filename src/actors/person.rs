use activitypub_federation::{
    config::Data,
    protocol::{public_key::PublicKey, verification::verify_domains_match},
    traits::Object,
};
use chrono::{DateTime, Utc};
use url::Url;

use crate::data::FederationData;
use crate::error::Error;
use crate::repository::RemoteActor;

use super::types::{ApImageObject, DbActor, Endpoints, Person, ProfileFieldObject};
use super::{apex_domain, build_local_actor};

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
        let user_id = match data.url_scheme.extract_user_id(&object_id) {
            Some(id) => id,
            None => return Ok(None),
        };
        if data
            .actor_repo
            .get_local_actor_keypair(user_id)
            .await?
            .is_none()
        {
            return Ok(None);
        }
        match build_local_actor(
            user_id,
            &data.base_url,
            data.user_repo.as_ref(),
            data.actor_repo.as_ref(),
            data.url_scheme.as_ref(),
        )
        .await
        {
            Ok(actor) => Ok(Some(actor)),
            Err(_) => Ok(None),
        }
    }

    async fn into_json(self, data: &Data<Self::DataType>) -> Result<Self::Kind, Self::Error> {
        let public_key = PublicKey {
            id: format!("{}#main-key", self.ap_id),
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
        let also_known_as = self.also_known_as;
        let attachment: Vec<ProfileFieldObject> = self
            .attachment
            .into_iter()
            .map(|field| ProfileFieldObject {
                kind: "PropertyValue".to_string(),
                name: field.name,
                value: field.value,
            })
            .collect();

        let shared_inbox = data
            .url_scheme
            .shared_inbox_url(&data.base_url)
            .ok_or_else(|| anyhow::anyhow!("invalid base_url for shared inbox"))?;

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

        let cached_actor = RemoteActor::from_ap_person(&json);
        data.actor_repo.upsert_remote_actor(cached_actor).await?;

        let url_str = json.id.inner().to_string();
        let user_id = uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_URL, url_str.as_bytes());
        let ap_id = json.id.inner().clone();
        let inbox_url = json.inbox.clone();
        let shared_inbox_url = json
            .endpoints
            .as_ref()
            .and_then(|endpoints| Url::parse(endpoints.shared_inbox.as_str()).ok());
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
            avatar_url: json.icon.as_ref().map(|icon| icon.url.clone()),
            banner_url: json.image.as_ref().map(|image| image.url.clone()),
            also_known_as: json.also_known_as,
            profile_url: json.url.clone(),
            attachment: json
                .attachment
                .iter()
                .map(|field| crate::user::ApProfileField {
                    name: field.name.clone(),
                    value: field.value.clone(),
                })
                .collect(),
            manually_approves_followers: json.manually_approves_followers,
            discoverable: json.discoverable.unwrap_or(false),
            actor_type: json.kind,
            featured_url: json.featured,
        })
    }
}
