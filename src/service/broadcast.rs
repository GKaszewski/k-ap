use activitypub_federation::{protocol::context::WithContext, traits::Object};
use url::Url;

use crate::{
    activities::{
        AddActivity, AnnounceActivity, CreateActivity, DeleteActivity, MoveActivity, UndoActivity,
        UpdateActivity,
    },
    actors::{DbActor, get_local_actor},
    data::FederationData,
    user::ApVisibility,
};

use super::ActivityPubService;
use super::types::{AddRef, AddRefObject, AnnounceRef, LikeRef, TombstoneRef};

// Re-export so existing `crate::service::broadcast::{Addressing, visibility_addressing}` paths keep working.
#[allow(unused_imports)]
pub(crate) use super::types::Addressing;
pub(crate) use super::types::visibility_addressing;

fn deterministic_activity_id(
    base_url: &str,
    prefix: &str,
    user_id: uuid::Uuid,
    object_url: &Url,
) -> anyhow::Result<Url> {
    let namespace_input = format!("{}/{}", user_id, object_url);
    let deterministic_id =
        uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_URL, namespace_input.as_bytes());
    Ok(Url::parse(&format!(
        "{}/activities/{}/{}",
        base_url, prefix, deterministic_id
    ))?)
}

impl ActivityPubService {
    pub async fn broadcast_announce_to_followers(
        &self,
        local_user_id: uuid::Uuid,
        object_ap_id: Url,
    ) -> anyhow::Result<()> {
        let announce_id =
            deterministic_activity_id(&self.base_url, "announce", local_user_id, &object_ap_id)?;
        let data = self.federation_config.to_request_data();
        let Some((local_actor, inboxes)) =
            self.accepted_follower_inboxes(&data, local_user_id).await?
        else {
            return Ok(());
        };

        let announce = AnnounceActivity {
            id: announce_id,
            kind: Default::default(),
            actor: local_actor.object_id(),
            object: object_ap_id,
            published: Some(chrono::Utc::now()),
            to: vec![crate::urls::AS_PUBLIC.to_string()],
            cc: vec![local_actor.followers_url.to_string()],
        };

        self.send_activity(&data, &local_actor, inboxes, announce)
            .await
    }

    pub async fn broadcast_undo_announce_to_followers(
        &self,
        local_user_id: uuid::Uuid,
        object_ap_id: Url,
    ) -> anyhow::Result<()> {
        let announce_id =
            deterministic_activity_id(&self.base_url, "announce", local_user_id, &object_ap_id)?;
        let data = self.federation_config.to_request_data();
        let Some((local_actor, inboxes)) =
            self.accepted_follower_inboxes(&data, local_user_id).await?
        else {
            return Ok(());
        };

        let undo = UndoActivity {
            id: data.url_scheme.activity_url(&self.base_url)?,
            kind: Default::default(),
            actor: local_actor.object_id(),
            object: serde_json::to_value(AnnounceRef {
                kind: "Announce",
                id: announce_id.to_string(),
                actor: local_actor.ap_id.to_string(),
                object: object_ap_id.to_string(),
            })?,
        };

        self.send_activity(&data, &local_actor, inboxes, undo).await
    }

    pub async fn broadcast_like_to_inbox(
        &self,
        liker_user_id: uuid::Uuid,
        object_ap_id: Url,
        author_inbox_url: Url,
    ) -> anyhow::Result<()> {
        let data = self.federation_config.to_request_data();
        let local_actor = get_local_actor(liker_user_id, &data).await?;
        let like_id =
            deterministic_activity_id(&self.base_url, "like", liker_user_id, &object_ap_id)?;

        let like = crate::activities::LikeActivity {
            id: like_id,
            kind: Default::default(),
            actor: local_actor.object_id(),
            object: object_ap_id,
        };

        self.send_activity(&data, &local_actor, vec![author_inbox_url], like)
            .await
    }

    pub async fn broadcast_undo_like_to_inbox(
        &self,
        liker_user_id: uuid::Uuid,
        object_ap_id: Url,
        author_inbox_url: Url,
    ) -> anyhow::Result<()> {
        let data = self.federation_config.to_request_data();
        let local_actor = get_local_actor(liker_user_id, &data).await?;
        let like_id =
            deterministic_activity_id(&self.base_url, "like", liker_user_id, &object_ap_id)?;

        let undo = UndoActivity {
            id: data.url_scheme.activity_url(&self.base_url)?,
            kind: Default::default(),
            actor: local_actor.object_id(),
            object: serde_json::to_value(LikeRef {
                kind: "Like",
                id: like_id.to_string(),
                actor: local_actor.ap_id.to_string(),
                object: object_ap_id.to_string(),
            })?,
        };

        self.send_activity(&data, &local_actor, vec![author_inbox_url], undo)
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
            id: data.url_scheme.activity_url(&self.base_url)?,
            kind: Default::default(),
            actor: local_actor.object_id(),
            object: serde_json::to_value(TombstoneRef {
                kind: "Tombstone",
                id: ap_id.to_string(),
            })?,
            to: vec![crate::urls::AS_PUBLIC.to_string()],
            cc: vec![local_actor.followers_url.to_string()],
        };

        self.send_activity(&data, &local_actor, inboxes, delete)
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
            actor: local_actor.object_id(),
            object,
            to: vec![crate::urls::AS_PUBLIC.to_string()],
            cc: vec![local_actor.followers_url.to_string()],
        };

        self.send_activity(&data, &local_actor, inboxes, add).await
    }

    pub async fn broadcast_undo_add_to_followers(
        &self,
        local_user_id: uuid::Uuid,
        object_ap_id: Url,
    ) -> anyhow::Result<()> {
        let data = self.federation_config.to_request_data();
        let Some((local_actor, inboxes)) =
            self.accepted_follower_inboxes(&data, local_user_id).await?
        else {
            return Ok(());
        };

        let undo = UndoActivity {
            id: data.url_scheme.activity_url(&self.base_url)?,
            kind: Default::default(),
            actor: local_actor.object_id(),
            object: serde_json::to_value(AddRef {
                kind: "Add",
                id: object_ap_id.to_string(),
                object: AddRefObject {
                    id: object_ap_id.to_string(),
                },
            })?,
        };

        self.send_activity(&data, &local_actor, inboxes, undo).await
    }

    /// Resolve the local actor, gather follower + mentioned inboxes, and compute
    /// `to`/`cc` addressing. Returns `None` when visibility is `Private` or there
    /// are no inboxes to deliver to.
    async fn prepare_addressed_broadcast(
        &self,
        local_user_id: uuid::Uuid,
        visibility: ApVisibility,
        mentioned_inboxes: Vec<Url>,
    ) -> anyhow::Result<
        Option<(
            activitypub_federation::config::Data<FederationData>,
            DbActor,
            Vec<Url>,
            Addressing,
        )>,
    > {
        if visibility == ApVisibility::Private {
            return Ok(None);
        }
        let data = self.federation_config.to_request_data();
        let local_actor = get_local_actor(local_user_id, &data).await?;
        let follower_inboxes = data
            .follow_repo
            .get_accepted_follower_inboxes(local_user_id)
            .await?;
        let inboxes = merge_inboxes(follower_inboxes, mentioned_inboxes);
        if inboxes.is_empty() {
            return Ok(None);
        }
        let addressing = visibility_addressing(visibility, &local_actor.followers_url);
        Ok(Some((data, local_actor, inboxes, addressing)))
    }

    /// Fan out a Create activity to accepted followers and any explicitly
    /// mentioned actors.
    ///
    /// `visibility` controls `to`/`cc` addressing:
    /// - `Public` / `FollowersOnly`: delivered to followers + `mentioned_inboxes`
    /// - `Private`: returns immediately — no delivery to anyone
    ///
    /// `mentioned_inboxes` should contain the inbox URLs of remote actors
    /// explicitly tagged in the object who are not already followers. Resolve them
    /// via [`ActivityPubService::lookup_actor_by_handle`] before calling. Pass an
    /// empty `Vec` if there are no external mentions.
    pub async fn broadcast_create(
        &self,
        local_user_id: uuid::Uuid,
        object: serde_json::Value,
        visibility: ApVisibility,
        mentioned_inboxes: Vec<Url>,
    ) -> anyhow::Result<()> {
        let Some((data, local_actor, inboxes, addressing)) = self
            .prepare_addressed_broadcast(local_user_id, visibility, mentioned_inboxes)
            .await?
        else {
            return Ok(());
        };

        let object_id_str = object["id"].as_str().unwrap_or("");
        let create_id = Url::parse(&format!(
            "{}/activities/create/{}",
            self.base_url,
            uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_URL, object_id_str.as_bytes())
        ))?;
        let create = CreateActivity {
            id: create_id,
            kind: Default::default(),
            actor: local_actor.object_id(),
            object,
            to: addressing.to,
            cc: addressing.cc,
            bto: vec![],
            bcc: vec![],
        };

        self.send_activity(&data, &local_actor, inboxes, create)
            .await
    }

    /// Fan out an Update activity to accepted followers and mentioned actors.
    /// See [`ActivityPubService::broadcast_create`] for `mentioned_inboxes` semantics.
    pub async fn broadcast_update(
        &self,
        local_user_id: uuid::Uuid,
        object: serde_json::Value,
        visibility: ApVisibility,
        mentioned_inboxes: Vec<Url>,
    ) -> anyhow::Result<()> {
        let Some((data, local_actor, inboxes, addressing)) = self
            .prepare_addressed_broadcast(local_user_id, visibility, mentioned_inboxes)
            .await?
        else {
            return Ok(());
        };

        let update = UpdateActivity {
            id: data.url_scheme.activity_url(&self.base_url)?,
            kind: Default::default(),
            actor: local_actor.object_id(),
            object,
            to: addressing.to,
            cc: addressing.cc,
        };

        self.send_activity(&data, &local_actor, inboxes, update)
            .await
    }

    pub async fn broadcast_actor_update(&self, user_id: uuid::Uuid) -> anyhow::Result<()> {
        let data = self.federation_config.to_request_data();
        let local_actor = get_local_actor(user_id, &data).await?;

        let person = local_actor.clone().into_json(&data).await?;
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
            actor: local_actor.object_id(),
            object: person_json,
            to: vec![crate::urls::AS_PUBLIC.to_string()],
            cc: vec![local_actor.followers_url.to_string()],
        };

        let Some((_, inboxes)) = self.accepted_follower_inboxes(&data, user_id).await? else {
            tracing::info!(%user_id, "no accepted followers, skipping actor update broadcast");
            return Ok(());
        };

        tracing::info!(%user_id, inbox_count = inboxes.len(), "broadcasting actor update");
        self.send_activity(&data, &local_actor, inboxes, update)
            .await
    }

    pub async fn broadcast_move(
        &self,
        user_id: uuid::Uuid,
        new_actor_url: Url,
    ) -> anyhow::Result<()> {
        let data = self.federation_config.to_request_data();
        let local_actor = get_local_actor(user_id, &data).await?;
        let Some((_, inboxes)) = self.accepted_follower_inboxes(&data, user_id).await? else {
            tracing::info!(%user_id, "broadcast_move: no accepted followers");
            return Ok(());
        };

        let move_activity = MoveActivity {
            id: data.url_scheme.activity_url(&self.base_url)?,
            kind: Default::default(),
            actor: local_actor.object_id(),
            object: local_actor.ap_id.clone(),
            target: new_actor_url.clone(),
        };

        self.send_activity(&data, &local_actor, inboxes, move_activity)
            .await?;
        tracing::info!(%user_id, target = %new_actor_url, "broadcast_move: dispatched");
        Ok(())
    }
    /// Broadcast a pre-built activity to all accepted followers.
    ///
    /// This is the low-level escape hatch for custom activity types that
    /// k-ap doesn't have a dedicated method for. The `activity` JSON must
    /// be a complete AP activity with `id`, `type`, `actor`, etc. already set.
    /// k-ap wraps it in `@context` and handles signing + delivery.
    pub async fn broadcast_raw_to_followers(
        &self,
        local_user_id: uuid::Uuid,
        activity: serde_json::Value,
    ) -> anyhow::Result<()> {
        let data = self.federation_config.to_request_data();
        let Some((local_actor, inboxes)) =
            self.accepted_follower_inboxes(&data, local_user_id).await?
        else {
            return Ok(());
        };

        self.send_raw_activity(&data, &local_actor, inboxes, activity)
            .await
    }
}

fn merge_inboxes(follower_inboxes: Vec<String>, mentioned_inboxes: Vec<Url>) -> Vec<Url> {
    let mut seen = std::collections::HashSet::new();
    let mut inboxes: Vec<Url> = follower_inboxes
        .into_iter()
        .filter_map(|inbox_str| Url::parse(&inbox_str).ok())
        .filter(|url| seen.insert(url.to_string()))
        .collect();
    for inbox in mentioned_inboxes {
        if seen.insert(inbox.to_string()) {
            inboxes.push(inbox);
        }
    }
    inboxes
}
