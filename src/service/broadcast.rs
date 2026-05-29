use activitypub_federation::{
    fetch::object_id::ObjectId, protocol::context::WithContext, traits::Object,
};
use url::Url;

use crate::{
    activities::{
        AddActivity, AnnounceActivity, CreateActivity, DeleteActivity, MoveActivity, UndoActivity,
        UpdateActivity,
    },
    actors::get_local_actor,
    urls::activity_url,
    user::ApVisibility,
};

use super::ActivityPubService;

impl ActivityPubService {
    pub async fn broadcast_announce_to_followers(
        &self,
        local_user_id: uuid::Uuid,
        object_ap_id: url::Url,
    ) -> anyhow::Result<()> {
        let announce_id = url::Url::parse(&format!(
            "{}/activities/announce/{}",
            self.base_url,
            uuid::Uuid::new_v5(
                &uuid::Uuid::NAMESPACE_URL,
                format!("{}/{}", local_user_id, object_ap_id).as_bytes()
            ),
        ))
        .map_err(|e| anyhow::anyhow!("{e}"))?;
        let data = self.federation_config.to_request_data();
        let Some((local_actor, inboxes)) =
            self.accepted_follower_inboxes(&data, local_user_id).await?
        else {
            return Ok(());
        };
        let announce = AnnounceActivity {
            id: announce_id,
            kind: Default::default(),
            actor: ObjectId::from(local_actor.ap_id.clone()),
            object: object_ap_id,
            published: Some(chrono::Utc::now()),
            to: vec![crate::urls::AS_PUBLIC.to_string()],
            cc: vec![local_actor.followers_url.to_string()],
        };
        let (json, sends, inboxes) = self
            .prepare_broadcast(&data, &local_actor, inboxes, announce)
            .await?;
        self.dispatch_deliveries(&data, &local_actor, inboxes, sends, json)
            .await
    }

    pub async fn broadcast_undo_announce_to_followers(
        &self,
        local_user_id: uuid::Uuid,
        object_ap_id: url::Url,
    ) -> anyhow::Result<()> {
        let announce_id = url::Url::parse(&format!(
            "{}/activities/announce/{}",
            self.base_url,
            uuid::Uuid::new_v5(
                &uuid::Uuid::NAMESPACE_URL,
                format!("{}/{}", local_user_id, object_ap_id).as_bytes()
            ),
        ))
        .map_err(|e| anyhow::anyhow!("{e}"))?;
        let undo_id = activity_url(&self.base_url).map_err(|e| anyhow::anyhow!("{e}"))?;
        let data = self.federation_config.to_request_data();
        let Some((local_actor, inboxes)) =
            self.accepted_follower_inboxes(&data, local_user_id).await?
        else {
            return Ok(());
        };
        let undo = UndoActivity {
            id: undo_id,
            kind: Default::default(),
            actor: ObjectId::from(local_actor.ap_id.clone()),
            object: serde_json::json!({"type":"Announce","id":announce_id.to_string(),"actor":local_actor.ap_id.to_string(),"object":object_ap_id.to_string()}),
        };
        let (json, sends, inboxes) = self
            .prepare_broadcast(&data, &local_actor, inboxes, undo)
            .await?;
        self.dispatch_deliveries(&data, &local_actor, inboxes, sends, json)
            .await
    }

    pub async fn broadcast_like_to_inbox(
        &self,
        liker_user_id: uuid::Uuid,
        object_ap_id: url::Url,
        author_inbox_url: url::Url,
    ) -> anyhow::Result<()> {
        let data = self.federation_config.to_request_data();
        let local_actor = get_local_actor(liker_user_id, &data)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let like_id = url::Url::parse(&format!(
            "{}/activities/like/{}",
            self.base_url,
            uuid::Uuid::new_v5(
                &uuid::Uuid::NAMESPACE_URL,
                format!("{}/{}", liker_user_id, object_ap_id).as_bytes()
            ),
        ))?;
        let like = crate::activities::LikeActivity {
            id: like_id,
            kind: Default::default(),
            actor: ObjectId::from(local_actor.ap_id.clone()),
            object: object_ap_id,
        };
        let (json, sends, inboxes) = self
            .prepare_broadcast(&data, &local_actor, vec![author_inbox_url], like)
            .await?;
        self.dispatch_deliveries(&data, &local_actor, inboxes, sends, json)
            .await
    }

    pub async fn broadcast_undo_like_to_inbox(
        &self,
        liker_user_id: uuid::Uuid,
        object_ap_id: url::Url,
        author_inbox_url: url::Url,
    ) -> anyhow::Result<()> {
        let data = self.federation_config.to_request_data();
        let local_actor = get_local_actor(liker_user_id, &data)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let like_id = url::Url::parse(&format!(
            "{}/activities/like/{}",
            self.base_url,
            uuid::Uuid::new_v5(
                &uuid::Uuid::NAMESPACE_URL,
                format!("{}/{}", liker_user_id, object_ap_id).as_bytes()
            ),
        ))?;
        let undo_id = activity_url(&self.base_url).map_err(|e| anyhow::anyhow!("{e}"))?;
        let undo = UndoActivity {
            id: undo_id,
            kind: Default::default(),
            actor: ObjectId::from(local_actor.ap_id.clone()),
            object: serde_json::json!({"type":"Like","id":like_id.to_string(),"actor":local_actor.ap_id.to_string(),"object":object_ap_id.to_string()}),
        };
        let (json, sends, inboxes) = self
            .prepare_broadcast(&data, &local_actor, vec![author_inbox_url], undo)
            .await?;
        self.dispatch_deliveries(&data, &local_actor, inboxes, sends, json)
            .await
    }

    pub async fn broadcast_delete_to_followers(
        &self,
        local_user_id: uuid::Uuid,
        ap_id: Url,
    ) -> anyhow::Result<()> {
        let data = self.federation_config.to_request_data();
        let Some((local_actor, inboxes)) =
            self.accepted_follower_inboxes(&data, local_user_id).await?
        else {
            return Ok(());
        };
        let delete = DeleteActivity {
            id: activity_url(&self.base_url).map_err(|e| anyhow::anyhow!("{e}"))?,
            kind: Default::default(),
            actor: ObjectId::from(local_actor.ap_id.clone()),
            object: serde_json::json!({"type": "Tombstone", "id": ap_id.to_string()}),
            to: vec![crate::urls::AS_PUBLIC.to_string()],
            cc: vec![local_actor.followers_url.to_string()],
        };
        let (json, sends, inboxes) = self
            .prepare_broadcast(&data, &local_actor, inboxes, delete)
            .await?;
        self.dispatch_deliveries(&data, &local_actor, inboxes, sends, json)
            .await
    }

    pub async fn broadcast_add_to_followers(
        &self,
        local_user_id: uuid::Uuid,
        ap_id: Url,
        object: serde_json::Value,
    ) -> anyhow::Result<()> {
        let data = self.federation_config.to_request_data();
        let Some((local_actor, inboxes)) =
            self.accepted_follower_inboxes(&data, local_user_id).await?
        else {
            return Ok(());
        };
        let add = AddActivity {
            id: ap_id,
            kind: Default::default(),
            actor: ObjectId::from(local_actor.ap_id.clone()),
            object,
            to: vec![crate::urls::AS_PUBLIC.to_string()],
            cc: vec![local_actor.followers_url.to_string()],
        };
        let (json, sends, inboxes) = self
            .prepare_broadcast(&data, &local_actor, inboxes, add)
            .await?;
        self.dispatch_deliveries(&data, &local_actor, inboxes, sends, json)
            .await
    }

    pub async fn broadcast_undo_add_to_followers(
        &self,
        local_user_id: uuid::Uuid,
        watchlist_entry_ap_id: Url,
    ) -> anyhow::Result<()> {
        let data = self.federation_config.to_request_data();
        let Some((local_actor, inboxes)) =
            self.accepted_follower_inboxes(&data, local_user_id).await?
        else {
            return Ok(());
        };
        let undo = UndoActivity {
            id: activity_url(&self.base_url).map_err(|e| anyhow::anyhow!("{e}"))?,
            kind: Default::default(),
            actor: ObjectId::from(local_actor.ap_id.clone()),
            object: serde_json::json!({"type":"Add","id":watchlist_entry_ap_id.as_str(),"object":{"id":watchlist_entry_ap_id.as_str()}}),
        };
        let (json, sends, inboxes) = self
            .prepare_broadcast(&data, &local_actor, inboxes, undo)
            .await?;
        self.dispatch_deliveries(&data, &local_actor, inboxes, sends, json)
            .await
    }

    /// Fan out a Create(Note) activity to accepted followers and any explicitly
    /// mentioned actors.
    ///
    /// `visibility` controls `to`/`cc` addressing and whether the note is public:
    /// - `Public` / `FollowersOnly`: delivered to followers + `mentioned_inboxes`
    /// - `Private`: returns immediately — no delivery to anyone
    ///
    /// `mentioned_inboxes` should contain the inbox URLs of remote actors
    /// explicitly tagged in the note who are not already followers. Resolve them
    /// via [`ActivityPubService::lookup_actor_by_handle`] before calling. Pass an
    /// empty `Vec` if there are no external mentions.
    pub async fn broadcast_create_note(
        &self,
        local_user_id: uuid::Uuid,
        note: serde_json::Value,
        visibility: ApVisibility,
        mentioned_inboxes: Vec<Url>,
    ) -> anyhow::Result<()> {
        if visibility == ApVisibility::Private {
            return Ok(());
        }
        let data = self.federation_config.to_request_data();
        let local_actor = crate::actors::get_local_actor(local_user_id, &data)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;

        // Merge follower inboxes with explicitly mentioned actor inboxes,
        // deduplicating by string to avoid delivering the same inbox twice.
        let follower_inboxes = data
            .follow_repo
            .get_accepted_follower_inboxes(local_user_id)
            .await?;
        let mut seen = std::collections::HashSet::new();
        let mut inboxes: Vec<Url> = follower_inboxes
            .into_iter()
            .filter_map(|s| Url::parse(&s).ok())
            .filter(|u| seen.insert(u.to_string()))
            .collect();
        for inbox in mentioned_inboxes {
            if seen.insert(inbox.to_string()) {
                inboxes.push(inbox);
            }
        }
        if inboxes.is_empty() {
            return Ok(());
        }

        let note_id_str = note["id"].as_str().unwrap_or("");
        let create_id = Url::parse(&format!(
            "{}/activities/create/{}",
            self.base_url,
            uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_URL, note_id_str.as_bytes())
        ))
        .map_err(|e| anyhow::anyhow!("{e}"))?;
        let (to, cc) = visibility_addressing(visibility, &local_actor.followers_url);
        let create = CreateActivity {
            id: create_id,
            kind: Default::default(),
            actor: ObjectId::from(local_actor.ap_id.clone()),
            object: note,
            to,
            cc,
            bto: vec![],
            bcc: vec![],
        };
        let (json, sends, inboxes) = self
            .prepare_broadcast(&data, &local_actor, inboxes, create)
            .await?;
        self.dispatch_deliveries(&data, &local_actor, inboxes, sends, json)
            .await
    }

    /// Fan out an Update(Note) activity to accepted followers and mentioned actors.
    /// See [`broadcast_create_note`] for `mentioned_inboxes` semantics.
    pub async fn broadcast_update_note(
        &self,
        local_user_id: uuid::Uuid,
        note: serde_json::Value,
        visibility: ApVisibility,
        mentioned_inboxes: Vec<Url>,
    ) -> anyhow::Result<()> {
        if visibility == ApVisibility::Private {
            return Ok(());
        }
        let data = self.federation_config.to_request_data();
        let local_actor = crate::actors::get_local_actor(local_user_id, &data)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;

        let follower_inboxes = data
            .follow_repo
            .get_accepted_follower_inboxes(local_user_id)
            .await?;
        let mut seen = std::collections::HashSet::new();
        let mut inboxes: Vec<Url> = follower_inboxes
            .into_iter()
            .filter_map(|s| Url::parse(&s).ok())
            .filter(|u| seen.insert(u.to_string()))
            .collect();
        for inbox in mentioned_inboxes {
            if seen.insert(inbox.to_string()) {
                inboxes.push(inbox);
            }
        }
        if inboxes.is_empty() {
            return Ok(());
        }

        let (to, cc) = visibility_addressing(visibility, &local_actor.followers_url);
        let update = crate::activities::UpdateActivity {
            id: activity_url(&self.base_url).map_err(|e| anyhow::anyhow!("{e}"))?,
            kind: Default::default(),
            actor: ObjectId::from(local_actor.ap_id.clone()),
            object: note,
            to,
            cc,
        };
        let (json, sends, inboxes) = self
            .prepare_broadcast(&data, &local_actor, inboxes, update)
            .await?;
        self.dispatch_deliveries(&data, &local_actor, inboxes, sends, json)
            .await
    }

    pub async fn broadcast_actor_update(&self, user_id: uuid::Uuid) -> anyhow::Result<()> {
        let data = self.federation_config.to_request_data();
        let local_actor = get_local_actor(user_id, &data)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let person = local_actor
            .clone()
            .into_json(&data)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let person_json =
            serde_json::to_value(WithContext::new(person, crate::urls::actor_ap_context()))?;
        let update_id = Url::parse(&format!(
            "{}/activities/update/{}",
            self.base_url,
            uuid::Uuid::new_v4()
        ))?;
        let update = UpdateActivity {
            id: update_id,
            kind: Default::default(),
            actor: ObjectId::from(local_actor.ap_id.clone()),
            object: person_json,
            to: vec![crate::urls::AS_PUBLIC.to_string()],
            cc: vec![local_actor.followers_url.to_string()],
        };
        let Some((_, inboxes)) = self.accepted_follower_inboxes(&data, user_id).await? else {
            tracing::info!(%user_id, "no accepted followers, skipping actor update broadcast");
            return Ok(());
        };
        tracing::info!(%user_id, inbox_count = inboxes.len(), "broadcasting actor update");
        let (json, sends, inboxes) = self
            .prepare_broadcast(&data, &local_actor, inboxes, update)
            .await?;
        self.dispatch_deliveries(&data, &local_actor, inboxes, sends, json)
            .await
    }

    pub async fn broadcast_move(
        &self,
        user_id: uuid::Uuid,
        new_actor_url: url::Url,
    ) -> anyhow::Result<()> {
        let data = self.federation_config.to_request_data();
        let local_actor = get_local_actor(user_id, &data)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let Some((_, inboxes)) = self.accepted_follower_inboxes(&data, user_id).await? else {
            tracing::info!(%user_id, "broadcast_move: no accepted followers");
            return Ok(());
        };
        let move_activity = MoveActivity {
            id: activity_url(&self.base_url).map_err(|e| anyhow::anyhow!("{e}"))?,
            kind: Default::default(),
            actor: ObjectId::from(local_actor.ap_id.clone()),
            object: local_actor.ap_id.clone(),
            target: new_actor_url.clone(),
        };
        let (json, sends, inboxes) = self
            .prepare_broadcast(&data, &local_actor, inboxes, move_activity)
            .await?;
        self.dispatch_deliveries(&data, &local_actor, inboxes, sends, json)
            .await?;
        tracing::info!(%user_id, target = %new_actor_url, "broadcast_move: dispatched");
        Ok(())
    }
}

/// Returns `(to, cc)` addressing for the given visibility.
/// `Private` is handled before calling this (early return in broadcast methods).
pub(super) fn visibility_addressing(
    visibility: ApVisibility,
    followers_url: &Url,
) -> (Vec<String>, Vec<String>) {
    match visibility {
        ApVisibility::Public => (
            vec![crate::urls::AS_PUBLIC.to_string()],
            vec![followers_url.to_string()],
        ),
        ApVisibility::FollowersOnly => (vec![followers_url.to_string()], vec![]),
        ApVisibility::Private => (vec![], vec![]),
    }
}
