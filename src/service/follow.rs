use activitypub_federation::{fetch::object_id::ObjectId, traits::Actor};
use url::Url;

use crate::{
    activities::{AcceptActivity, FollowActivity, RejectActivity, UndoActivity},
    actors::get_local_actor,
    data::FederationData,
    repository::{FollowerStatus, FollowingStatus, RemoteActor},
    urls::activity_url,
};

use super::ActivityPubService;

impl ActivityPubService {
    pub async fn follow(&self, local_user_id: uuid::Uuid, handle: &str) -> anyhow::Result<()> {
        let data = self.federation_config.to_request_data();
        let normalized = handle.trim_start_matches('@');
        let parts: Vec<&str> = normalized.splitn(2, '@').collect();
        if parts.len() == 2 && parts[1] == data.domain {
            return self.follow_local(local_user_id, parts[0], &data).await;
        }
        let remote_actor = self.webfinger_https(handle, &data).await?;
        let local_actor = get_local_actor(local_user_id, &data)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let follow_id = activity_url(&self.base_url).map_err(|e| anyhow::anyhow!("{e}"))?;
        let follow_id_str = follow_id.to_string();
        let remote = RemoteActor {
            url: remote_actor.ap_id.to_string(),
            handle: format!(
                "{}@{}",
                remote_actor.username,
                remote_actor.ap_id.host_str().unwrap_or("")
            ),
            inbox_url: remote_actor.inbox_url.to_string(),
            shared_inbox_url: remote_actor
                .shared_inbox_url
                .as_ref()
                .map(|u| u.to_string()),
            display_name: Some(remote_actor.username.clone()),
            avatar_url: remote_actor.avatar_url.as_ref().map(|u| u.to_string()),
            outbox_url: Some(remote_actor.outbox_url.to_string()),
        };
        // Save BEFORE delivering — prevents lost state on process restart.
        data.follow_repo
            .add_following(local_user_id, remote, &follow_id_str)
            .await?;
        let follow = FollowActivity {
            id: Url::parse(&follow_id_str)?,
            kind: Default::default(),
            actor: ObjectId::from(local_actor.ap_id.clone()),
            object: ObjectId::from(remote_actor.ap_id.clone()),
        };
        let (json, sends, inboxes) = self
            .prepare_broadcast(&data, &local_actor, vec![remote_actor.inbox()], follow)
            .await?;
        self.dispatch_deliveries(&data, &local_actor, inboxes, sends, json)
            .await
    }

    pub async fn unfollow(
        &self,
        local_user_id: uuid::Uuid,
        actor_url_str: &str,
    ) -> anyhow::Result<()> {
        let data = self.federation_config.to_request_data();
        if actor_url_str.starts_with(&self.base_url) {
            return self
                .unfollow_local(local_user_id, actor_url_str, &data)
                .await;
        }
        let remote = data
            .actor_repo
            .get_remote_actor(actor_url_str)
            .await?
            .ok_or_else(|| anyhow::anyhow!("remote actor not found: {}", actor_url_str))?;
        let local_actor = get_local_actor(local_user_id, &data)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let remote_ap_id = Url::parse(actor_url_str)?;
        let inbox = Url::parse(&remote.inbox_url)?;
        let follow_id = data
            .follow_repo
            .get_follow_activity_id(local_user_id, actor_url_str)
            .await?
            .and_then(|id| Url::parse(&id).ok())
            .unwrap_or_else(|| {
                activity_url(&self.base_url).unwrap_or_else(|_| remote_ap_id.clone())
            });
        let follow = FollowActivity {
            id: follow_id,
            kind: Default::default(),
            actor: ObjectId::from(local_actor.ap_id.clone()),
            object: ObjectId::from(remote_ap_id),
        };
        let undo = UndoActivity {
            id: activity_url(&self.base_url).map_err(|e| anyhow::anyhow!("{e}"))?,
            kind: Default::default(),
            actor: ObjectId::from(local_actor.ap_id.clone()),
            object: serde_json::to_value(&follow).map_err(|e| anyhow::anyhow!("{e}"))?,
        };
        let (json, sends, inboxes) = self
            .prepare_broadcast(&data, &local_actor, vec![inbox], undo)
            .await?;
        self.dispatch_deliveries(&data, &local_actor, inboxes, sends, json)
            .await?;
        data.follow_repo
            .remove_following(local_user_id, actor_url_str)
            .await?;
        data.object_handler
            .on_actor_removed(&Url::parse(actor_url_str)?)
            .await?;
        Ok(())
    }

    pub async fn accept_follower(
        &self,
        local_user_id: uuid::Uuid,
        remote_actor_url: &str,
    ) -> anyhow::Result<()> {
        let data = self.federation_config.to_request_data();
        let local_actor = get_local_actor(local_user_id, &data)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let remote_actor = data
            .actor_repo
            .get_remote_actor(remote_actor_url)
            .await?
            .ok_or_else(|| anyhow::anyhow!("remote actor not found"))?;
        let follow_id_str = data
            .follow_repo
            .get_follower_follow_activity_id(local_user_id, remote_actor_url)
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!("follow activity id not found for {}", remote_actor_url)
            })?;
        let follow = FollowActivity {
            id: Url::parse(&follow_id_str)?,
            kind: Default::default(),
            actor: ObjectId::from(Url::parse(remote_actor_url)?),
            object: ObjectId::from(local_actor.ap_id.clone()),
        };
        let accept = AcceptActivity {
            id: activity_url(&self.base_url).map_err(|e| anyhow::anyhow!("{e}"))?,
            kind: Default::default(),
            actor: ObjectId::from(local_actor.ap_id.clone()),
            object: follow,
        };
        data.follow_repo
            .update_follower_status(local_user_id, remote_actor_url, FollowerStatus::Accepted)
            .await?;
        let inbox = Url::parse(&remote_actor.inbox_url)?;
        let (json, sends, inboxes) = self
            .prepare_broadcast(&data, &local_actor, vec![inbox], accept)
            .await?;
        self.dispatch_deliveries(&data, &local_actor, inboxes, sends, json)
            .await?;
        let target_inbox = remote_actor
            .shared_inbox_url
            .clone()
            .unwrap_or_else(|| remote_actor.inbox_url.clone());
        self.spawn_backfill(local_user_id, target_inbox);
        Ok(())
    }

    pub async fn reject_follower(
        &self,
        local_user_id: uuid::Uuid,
        remote_actor_url: &str,
    ) -> anyhow::Result<()> {
        let data = self.federation_config.to_request_data();
        let local_actor = get_local_actor(local_user_id, &data)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let remote_actor = data
            .actor_repo
            .get_remote_actor(remote_actor_url)
            .await?
            .ok_or_else(|| anyhow::anyhow!("remote actor not found"))?;
        let follow = FollowActivity {
            id: activity_url(&self.base_url).map_err(|e| anyhow::anyhow!("{e}"))?,
            kind: Default::default(),
            actor: ObjectId::from(Url::parse(remote_actor_url)?),
            object: ObjectId::from(local_actor.ap_id.clone()),
        };
        let reject = RejectActivity {
            id: activity_url(&self.base_url).map_err(|e| anyhow::anyhow!("{e}"))?,
            kind: Default::default(),
            actor: ObjectId::from(local_actor.ap_id.clone()),
            object: follow,
        };
        let inbox = Url::parse(&remote_actor.inbox_url)?;
        let (json, sends, inboxes) = self
            .prepare_broadcast(&data, &local_actor, vec![inbox], reject)
            .await?;
        self.dispatch_deliveries(&data, &local_actor, inboxes, sends, json)
            .await?;
        data.follow_repo
            .remove_follower(local_user_id, remote_actor_url)
            .await?;
        Ok(())
    }

    pub async fn get_pending_followers(
        &self,
        local_user_id: uuid::Uuid,
    ) -> anyhow::Result<Vec<RemoteActor>> {
        let data = self.federation_config.to_request_data();
        data.follow_repo.get_pending_followers(local_user_id).await
    }

    pub async fn get_accepted_followers(
        &self,
        local_user_id: uuid::Uuid,
    ) -> anyhow::Result<Vec<RemoteActor>> {
        let data = self.federation_config.to_request_data();
        Ok(data
            .follow_repo
            .get_followers(local_user_id)
            .await?
            .into_iter()
            .filter(|f| f.status == FollowerStatus::Accepted)
            .map(|f| f.actor)
            .collect())
    }

    pub async fn count_accepted_followers(
        &self,
        local_user_id: uuid::Uuid,
    ) -> anyhow::Result<usize> {
        let data = self.federation_config.to_request_data();
        Ok(data
            .follow_repo
            .get_followers(local_user_id)
            .await?
            .into_iter()
            .filter(|f| f.status == FollowerStatus::Accepted)
            .count())
    }

    pub async fn get_following(
        &self,
        local_user_id: uuid::Uuid,
    ) -> anyhow::Result<Vec<RemoteActor>> {
        let data = self.federation_config.to_request_data();
        data.follow_repo.get_following(local_user_id).await
    }

    pub async fn count_following(&self, local_user_id: uuid::Uuid) -> anyhow::Result<usize> {
        let data = self.federation_config.to_request_data();
        data.follow_repo.count_following(local_user_id).await
    }

    pub async fn remove_follower(
        &self,
        local_user_id: uuid::Uuid,
        actor_url: &str,
    ) -> anyhow::Result<()> {
        let data = self.federation_config.to_request_data();
        data.follow_repo
            .remove_follower(local_user_id, actor_url)
            .await
    }

    pub async fn block_actor(
        &self,
        local_user_id: uuid::Uuid,
        actor_url: &str,
    ) -> anyhow::Result<()> {
        let data = self.federation_config.to_request_data();
        data.blocklist_repo
            .add_blocked_actor(local_user_id, actor_url)
            .await?;
        let _ = data
            .follow_repo
            .remove_follower(local_user_id, actor_url)
            .await;
        let _ = data
            .follow_repo
            .remove_following(local_user_id, actor_url)
            .await;
        let local_actor = get_local_actor(local_user_id, &data)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        if let Ok(Some(remote_actor)) = data.actor_repo.get_remote_actor(actor_url).await {
            let block = crate::activities::BlockActivity {
                id: activity_url(&self.base_url).map_err(|e| anyhow::anyhow!("{e}"))?,
                kind: Default::default(),
                actor: ObjectId::from(local_actor.ap_id.clone()),
                object: Url::parse(actor_url)?,
            };
            let inbox = Url::parse(&remote_actor.inbox_url)?;
            let (json, sends, inboxes) = self
                .prepare_broadcast(&data, &local_actor, vec![inbox], block)
                .await?;
            self.dispatch_deliveries(&data, &local_actor, inboxes, sends, json)
                .await?;
        }
        Ok(())
    }

    pub async fn unblock_actor(
        &self,
        local_user_id: uuid::Uuid,
        actor_url: &str,
    ) -> anyhow::Result<()> {
        let data = self.federation_config.to_request_data();
        data.blocklist_repo
            .remove_blocked_actor(local_user_id, actor_url)
            .await
    }

    pub async fn get_blocked_actors(
        &self,
        local_user_id: uuid::Uuid,
    ) -> anyhow::Result<Vec<RemoteActor>> {
        let data = self.federation_config.to_request_data();
        let actor_urls = data
            .blocklist_repo
            .get_blocked_actors(local_user_id)
            .await?;
        let mut actors = Vec::new();
        for url in actor_urls {
            let actor = match data.actor_repo.get_remote_actor(&url).await {
                Ok(Some(a)) => a,
                _ => RemoteActor {
                    url: url.clone(),
                    handle: url.clone(),
                    inbox_url: url.clone(),
                    shared_inbox_url: None,
                    display_name: None,
                    avatar_url: None,
                    outbox_url: None,
                },
            };
            actors.push(actor);
        }
        Ok(actors)
    }

    pub(super) async fn follow_local(
        &self,
        local_user_id: uuid::Uuid,
        target_username: &str,
        data: &activitypub_federation::config::Data<FederationData>,
    ) -> anyhow::Result<()> {
        let target = data
            .user_repo
            .find_by_username(target_username)
            .await?
            .ok_or_else(|| anyhow::anyhow!("user not found: {}", target_username))?;
        if target.id == local_user_id {
            return Err(anyhow::anyhow!("cannot follow yourself"));
        }
        let follower_actor_url = crate::urls::actor_url(&self.base_url, local_user_id).to_string();
        let target_actor_url = crate::urls::actor_url(&self.base_url, target.id);
        let follow_id = activity_url(&self.base_url)
            .map_err(|e| anyhow::anyhow!("{e}"))?
            .to_string();
        data.follow_repo
            .add_follower(
                target.id,
                &follower_actor_url,
                FollowerStatus::Accepted,
                &follow_id,
            )
            .await?;
        let target_as_remote = RemoteActor {
            url: target_actor_url.to_string(),
            handle: format!("{}@{}", target.username, data.domain),
            inbox_url: format!("{}/inbox", target_actor_url),
            shared_inbox_url: None,
            display_name: Some(target.username),
            avatar_url: None,
            outbox_url: None,
        };
        data.follow_repo
            .add_following(local_user_id, target_as_remote, &follow_id)
            .await?;
        data.follow_repo
            .update_following_status(
                local_user_id,
                target_actor_url.as_ref(),
                FollowingStatus::Accepted,
            )
            .await?;
        tracing::info!(follower = %local_user_id, followee = %target.id, "local follow");
        Ok(())
    }

    pub(super) async fn unfollow_local(
        &self,
        local_user_id: uuid::Uuid,
        target_actor_url: &str,
        data: &activitypub_federation::config::Data<FederationData>,
    ) -> anyhow::Result<()> {
        let target_url = Url::parse(target_actor_url)?;
        let target_user_id = crate::urls::extract_user_id_from_url(&target_url)
            .ok_or_else(|| anyhow::anyhow!("invalid local actor URL: {}", target_actor_url))?;
        let local_actor_url = crate::urls::actor_url(&self.base_url, local_user_id).to_string();
        data.follow_repo
            .remove_follower(target_user_id, &local_actor_url)
            .await?;
        data.follow_repo
            .remove_following(local_user_id, target_actor_url)
            .await?;
        tracing::info!(follower = %local_user_id, followee = %target_user_id, "local unfollow");
        Ok(())
    }
}
