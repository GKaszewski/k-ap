use url::Url;

/// Defines how ActivityPub URLs are constructed for local actors.
///
/// Implement this trait to use custom URL patterns (e.g. `/@username`
/// instead of `/users/{uuid}`). The default implementation
/// [`DefaultUrlScheme`] preserves the original `/users/{uuid}` layout.
pub trait UrlScheme: Send + Sync {
    fn actor_url(&self, base_url: &str, user_id: uuid::Uuid) -> anyhow::Result<Url>;
    fn inbox_url(&self, actor_url: &Url) -> anyhow::Result<Url>;
    fn shared_inbox_url(&self, base_url: &str) -> Option<Url>;
    fn outbox_url(&self, actor_url: &Url) -> anyhow::Result<Url>;
    fn followers_url(&self, actor_url: &Url) -> anyhow::Result<Url>;
    fn following_url(&self, actor_url: &Url) -> anyhow::Result<Url>;
    fn activity_url(&self, base_url: &str) -> anyhow::Result<Url>;
    fn extract_user_id(&self, url: &Url) -> Option<uuid::Uuid>;
}

/// Default URL scheme: `/users/{uuid}` with sub-paths for inbox, outbox, etc.
pub struct DefaultUrlScheme;

impl UrlScheme for DefaultUrlScheme {
    fn actor_url(&self, base_url: &str, user_id: uuid::Uuid) -> anyhow::Result<Url> {
        Url::parse(&format!("{}/users/{}", base_url, user_id))
            .map_err(|error| anyhow::anyhow!("invalid base_url: {error}"))
    }

    fn inbox_url(&self, actor_url: &Url) -> anyhow::Result<Url> {
        Url::parse(&format!("{}/inbox", actor_url))
            .map_err(|error| anyhow::anyhow!("invalid actor_url: {error}"))
    }

    fn shared_inbox_url(&self, base_url: &str) -> Option<Url> {
        Url::parse(&format!("{}/inbox", base_url)).ok()
    }

    fn outbox_url(&self, actor_url: &Url) -> anyhow::Result<Url> {
        Url::parse(&format!("{}/outbox", actor_url))
            .map_err(|error| anyhow::anyhow!("invalid actor_url: {error}"))
    }

    fn followers_url(&self, actor_url: &Url) -> anyhow::Result<Url> {
        Url::parse(&format!("{}/followers", actor_url))
            .map_err(|error| anyhow::anyhow!("invalid actor_url: {error}"))
    }

    fn following_url(&self, actor_url: &Url) -> anyhow::Result<Url> {
        Url::parse(&format!("{}/following", actor_url))
            .map_err(|error| anyhow::anyhow!("invalid actor_url: {error}"))
    }

    fn activity_url(&self, base_url: &str) -> anyhow::Result<Url> {
        Url::parse(&format!("{}/activities/{}", base_url, uuid::Uuid::new_v4()))
            .map_err(|error| anyhow::anyhow!("invalid base_url: {error}"))
    }

    fn extract_user_id(&self, url: &Url) -> Option<uuid::Uuid> {
        let path = url.path();
        path.strip_prefix("/users/")
            .and_then(|s| s.split('/').next())
            .and_then(|s| uuid::Uuid::parse_str(s).ok())
    }
}
