use chrono::{DateTime, Utc};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FollowerStatus {
    Pending,
    Accepted,
    Rejected,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FollowingStatus {
    Pending,
    Accepted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteActor {
    pub url: String,
    pub handle: String,
    pub inbox_url: String,
    pub shared_inbox_url: Option<String>,
    pub display_name: Option<String>,
    pub avatar_url: Option<String>,
    pub outbox_url: Option<String>,
    pub bio: Option<String>,
    pub banner_url: Option<String>,
    pub followers_url: Option<String>,
    pub following_url: Option<String>,
    pub also_known_as: Vec<String>,
    /// When this actor was last fetched from the origin instance.
    /// `None` means unknown — treated as always-fresh to avoid
    /// breaking existing consumers that don't populate this field.
    pub fetched_at: Option<DateTime<Utc>>,
}

impl From<&crate::actors::DbActor> for RemoteActor {
    fn from(actor: &crate::actors::DbActor) -> Self {
        Self {
            url: actor.ap_id.to_string(),
            handle: format!(
                "{}@{}",
                actor.username,
                actor.ap_id.host_str().unwrap_or("")
            ),
            inbox_url: actor.inbox_url.to_string(),
            shared_inbox_url: actor.shared_inbox_url.as_ref().map(|url| url.to_string()),
            display_name: actor
                .display_name
                .clone()
                .or_else(|| Some(actor.username.clone())),
            avatar_url: actor.avatar_url.as_ref().map(|url| url.to_string()),
            outbox_url: Some(actor.outbox_url.to_string()),
            bio: actor.bio.clone(),
            banner_url: actor.banner_url.as_ref().map(|url| url.to_string()),
            followers_url: Some(actor.followers_url.to_string()),
            following_url: Some(actor.following_url.to_string()),
            also_known_as: actor.also_known_as.clone(),
            fetched_at: Some(Utc::now()),
        }
    }
}

impl RemoteActor {
    pub fn from_ap_person(person: &crate::actors::Person) -> Self {
        Self {
            url: person.id.inner().to_string(),
            handle: person.preferred_username.clone(),
            inbox_url: person.inbox.to_string(),
            shared_inbox_url: person
                .endpoints
                .as_ref()
                .map(|endpoints| endpoints.shared_inbox.to_string()),
            display_name: person.name.clone(),
            avatar_url: person.icon.as_ref().map(|icon| icon.url.to_string()),
            outbox_url: person.outbox.as_ref().map(|url| url.to_string()),
            bio: person.summary.clone(),
            banner_url: person.image.as_ref().map(|image| image.url.to_string()),
            followers_url: person.followers.as_ref().map(|url| url.to_string()),
            following_url: person.following.as_ref().map(|url| url.to_string()),
            also_known_as: person.also_known_as.clone(),
            fetched_at: Some(Utc::now()),
        }
    }

    pub fn placeholder(actor_url: String) -> Self {
        Self {
            handle: actor_url.clone(),
            inbox_url: actor_url.clone(),
            shared_inbox_url: None,
            display_name: None,
            avatar_url: None,
            outbox_url: None,
            bio: None,
            banner_url: None,
            followers_url: None,
            following_url: None,
            also_known_as: vec![],
            fetched_at: None,
            url: actor_url,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Follower {
    pub actor: RemoteActor,
    pub status: FollowerStatus,
}

#[derive(Debug, Clone)]
pub struct BlockedDomain {
    pub domain: String,
    pub reason: Option<String>,
    pub blocked_at: String,
}

#[derive(Debug, Clone)]
pub struct Keypair {
    pub public_key: String,
    pub private_key: String,
}
