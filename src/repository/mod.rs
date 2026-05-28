mod activity;
mod actor;
mod blocklist;
mod follow;

pub use activity::ActivityRepository;
pub use actor::ActorRepository;
pub use blocklist::BlocklistRepository;
pub use follow::FollowRepository;

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
