use async_trait::async_trait;
use chrono::{DateTime, Utc};
use url::Url;

#[derive(Debug, Clone)]
pub struct LocalObject {
    pub ap_id: Url,
    pub object: serde_json::Value,
    pub published_at: DateTime<Utc>,
    pub to: Vec<String>,
    pub cc: Vec<String>,
    pub bto: Vec<String>,
    pub bcc: Vec<String>,
}

/// Read side — the library queries this when sending content outward.
/// Implement on the same struct as [`ApObjectHandler`] if you prefer a single
/// database type.
#[async_trait]
pub trait ApContentReader: Send + Sync {
    /// Newest-first page of locally-authored objects for `user_id`, published
    /// strictly before `before` (pass `None` for the first page).
    ///
    /// Used by the outbox endpoint and by backfill when a new follower is
    /// accepted. Implementations MUST:
    /// - Return objects in descending `published_at` order.
    /// - Exclude deleted and draft content.
    /// - Be consistent across pages (no duplicates, no gaps).
    async fn get_local_objects_page(
        &self,
        user_id: uuid::Uuid,
        before: Option<DateTime<Utc>>,
        limit: usize,
    ) -> anyhow::Result<Vec<LocalObject>>;

    /// Total locally-authored posts across all users. Used by NodeInfo.
    async fn count_local_posts(&self) -> anyhow::Result<u64>;

    /// AP URLs of pinned (featured) objects for this user, in display order.
    ///
    /// Served at `GET /users/{id}/featured` as an `OrderedCollection`.
    /// Mastodon and Pleroma follow this link from the actor's `featured` field.
    ///
    /// Defaults to an empty list — override to expose pinned posts.
    async fn get_featured_objects(&self, user_id: uuid::Uuid) -> anyhow::Result<Vec<url::Url>> {
        let _ = user_id;
        Ok(vec![])
    }
}

/// Write side — the library calls these when processing inbound AP activities.
///
/// All methods are called after HTTP signature verification has passed.
/// Returning `Err` propagates a 500 back to the remote server, which will
/// trigger a retry from well-behaved implementations. Return `Ok(())` to
/// silently accept an activity you don't want to act on.
///
/// **Idempotency:** Methods may be called more than once for the same activity
/// (e.g. under a race during duplicate delivery). Implementations should be
/// idempotent — prefer upsert over insert.
#[async_trait]
pub trait ApObjectHandler: Send + Sync {
    /// A remote actor published new content.
    ///
    /// `ap_id` is the stable URL of the object (e.g. the Note URL, not the
    /// Create activity URL). Store or index the `object` JSON as appropriate
    /// for your domain.
    async fn on_create(
        &self,
        ap_id: &Url,
        actor_url: &Url,
        object: serde_json::Value,
    ) -> anyhow::Result<()>;

    /// A remote actor edited existing content.
    ///
    /// `ap_id` matches a previously received `on_create` call. Update the
    /// stored object.
    async fn on_update(
        &self,
        ap_id: &Url,
        actor_url: &Url,
        object: serde_json::Value,
    ) -> anyhow::Result<()>;

    /// A remote actor deleted an object previously delivered via `on_create`.
    async fn on_delete(&self, ap_id: &Url, actor_url: &Url) -> anyhow::Result<()>;

    /// A remote actor was deleted or has unfollowed all local users.
    ///
    /// Remove all content and state associated with `actor_url` from local
    /// storage. Called for `Delete(actor)` and for `Undo(Follow)`.
    async fn on_actor_removed(&self, actor_url: &Url) -> anyhow::Result<()>;

    /// A remote actor liked a locally-authored object.
    async fn on_like(&self, object_url: &Url, actor_url: &Url) -> anyhow::Result<()>;

    /// A remote actor removed their like from a locally-authored object.
    async fn on_unlike(&self, object_url: &Url, actor_url: &Url) -> anyhow::Result<()>;

    /// A remote actor boosted (Announced) a **locally-authored** object.
    ///
    /// `object_url` is your local object's AP URL. The boost count is tracked
    /// separately in [`crate::repository::AnnounceRepository::count_announces`].
    async fn on_announce_received(&self, object_url: &Url, actor_url: &Url) -> anyhow::Result<()>;

    /// A remote actor removed their boost (`Undo(Announce)`) of a locally-authored
    /// object. Use this to decrement boost counts or update UI.
    ///
    /// Has a default no-op implementation — override to handle undone boosts.
    async fn on_announce_removed(&self, object_url: &Url, actor_url: &Url) -> anyhow::Result<()> {
        let _ = (object_url, actor_url);
        Ok(())
    }

    /// A remote actor boosted an object hosted on a **different server**.
    ///
    /// Use this to surface cross-server boosts in local feeds. Called instead
    /// of `on_announce_received` when the announced object URL is external.
    /// Failures are logged and swallowed — they do not fail the activity.
    async fn on_announce_of_remote(&self, object_url: &Url, actor_url: &Url) -> anyhow::Result<()>;

    /// A local user was tagged (Mentioned) in an inbound Create or Update.
    ///
    /// Called for every `{"type":"Mention","href":"<local-actor-url>"}` tag
    /// found in inbound activities. Use this to send in-app notifications.
    /// The note content is also delivered independently via `on_create`.
    ///
    /// Failures are logged and swallowed — a broken notification must not
    /// cause the activity to be rejected.
    async fn on_mention(
        &self,
        thought_ap_id: &Url,
        mentioned_user_uuid: uuid::Uuid,
        actor_url: &Url,
    ) -> anyhow::Result<()>;

    /// An inbound activity with an unrecognized type was received.
    ///
    /// Override this to handle custom ActivityPub extensions (EmojiReact,
    /// Question, Flag, etc.) that k-ap doesn't process natively.
    /// The raw JSON and the sender's actor URL are provided.
    ///
    /// **Note:** The default `router()` inbox handler gracefully accepts unknown
    /// activity types but cannot dispatch to this method due to upstream library
    /// constraints (the raw body is consumed during signature verification).
    /// To fully handle unknown activities, build a custom inbox handler that
    /// pre-parses the body before passing to `receive_activity`.
    ///
    /// Default is a no-op — unknown activities are silently accepted.
    async fn on_unknown_activity(
        &self,
        activity_type: &str,
        activity: serde_json::Value,
        actor_url: &Url,
    ) -> anyhow::Result<()> {
        let _ = (activity_type, activity, actor_url);
        Ok(())
    }
}
