mod person;
mod types;

pub use types::{DbActor, Person};

use activitypub_federation::{
    config::Data, http_signatures::generate_actor_keypair, traits::Actor,
};
use chrono::Utc;
use url::Url;
use zeroize::Zeroizing;

use crate::data::FederationData;
use crate::error::Error;

use types::ActorUrls;

pub async fn get_local_actor(
    user_id: uuid::Uuid,
    data: &Data<FederationData>,
) -> Result<DbActor, Error> {
    build_local_actor(
        user_id,
        &data.base_url,
        data.user_repo.as_ref(),
        data.actor_repo.as_ref(),
        data.url_scheme.as_ref(),
    )
    .await
    .map_err(|error| Error::not_found(error.to_string()))
}

/// Build a local actor's `DbActor` from repository data. Generates a keypair
/// if one doesn't exist yet. Usable outside of a `FederationData` context
/// (e.g. during service construction).
pub async fn build_local_actor(
    user_id: uuid::Uuid,
    base_url: &str,
    user_repo: &dyn crate::user::ApUserRepository,
    actor_repo: &dyn crate::repository::ActorRepository,
    url_scheme: &dyn crate::url_scheme::UrlScheme,
) -> anyhow::Result<DbActor> {
    let user = user_repo
        .find_by_id(user_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("user not found: {}", user_id))?;

    let keypair = match actor_repo.get_local_actor_keypair(user_id).await? {
        Some(existing) => existing,
        None => {
            let generated = generate_actor_keypair()?;
            let keypair = crate::repository::Keypair {
                public_key: generated.public_key,
                private_key: generated.private_key.clone(),
            };
            let private_zeroized = Zeroizing::new(generated.private_key);
            actor_repo
                .save_local_actor_keypair(user_id, keypair.clone())
                .await?;
            drop(private_zeroized);
            keypair
        }
    };

    let ActorUrls {
        ap_id,
        inbox_url,
        shared_inbox_url,
        outbox_url,
        followers_url,
        following_url,
    } = ActorUrls::build(base_url, user_id, url_scheme)?;

    Ok(DbActor {
        user_id,
        username: user.username,
        display_name: user.display_name,
        public_key_pem: keypair.public_key,
        private_key_pem: Some(keypair.private_key),
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
