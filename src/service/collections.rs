use activitypub_federation::{protocol::context::WithContext, traits::Object};

use crate::actors::get_local_actor;

use super::ActivityPubService;

impl ActivityPubService {
    pub async fn actor_json(&self, user_id_str: &str) -> anyhow::Result<String> {
        let uuid = uuid::Uuid::parse_str(user_id_str)?;
        let data = self.federation_config.to_request_data();
        let actor = get_local_actor(uuid, &data).await?;
        let person = actor.into_json(&data).await?;
        Ok(serde_json::to_string(&WithContext::new(
            person,
            crate::urls::actor_ap_context(),
        ))?)
    }

    pub async fn followers_collection_json(
        &self,
        user_id: uuid::Uuid,
        page: Option<u32>,
    ) -> anyhow::Result<String> {
        let data = self.federation_config.to_request_data();
        let actor_url = data.url_scheme.actor_url(&self.base_url, user_id)?;
        let collection_url = data.url_scheme.followers_url(&actor_url)?.to_string();
        let total = data.follow_repo.count_followers(user_id).await?;
        let items_fn = |offset: u32, limit: usize| {
            let data = data.clone();
            async move {
                Ok(data
                    .follow_repo
                    .get_followers_page(user_id, offset, limit)
                    .await?
                    .into_iter()
                    .map(|follower| follower.actor.url)
                    .collect())
            }
        };
        serialize_ordered_collection(&collection_url, total, page, items_fn).await
    }

    pub async fn following_collection_json(
        &self,
        user_id: uuid::Uuid,
        page: Option<u32>,
    ) -> anyhow::Result<String> {
        let data = self.federation_config.to_request_data();
        let actor_url = data.url_scheme.actor_url(&self.base_url, user_id)?;
        let collection_url = data.url_scheme.following_url(&actor_url)?.to_string();
        let total = data.follow_repo.count_following(user_id).await?;
        let items_fn = |offset: u32, limit: usize| {
            let data = data.clone();
            async move {
                Ok(data
                    .follow_repo
                    .get_following_page(user_id, offset, limit)
                    .await?
                    .into_iter()
                    .map(|actor| actor.url)
                    .collect())
            }
        };
        serialize_ordered_collection(&collection_url, total, page, items_fn).await
    }
}

pub(crate) async fn serialize_ordered_collection<F, Fut>(
    collection_url: &str,
    total: usize,
    page: Option<u32>,
    fetch_items: F,
) -> anyhow::Result<String>
where
    F: FnOnce(u32, usize) -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<Vec<String>>>,
{
    use crate::urls::{AP_CONTEXT, AP_PAGE_SIZE};

    let json = if let Some(page_number) = page {
        let page_number = page_number.max(1);
        let offset = (page_number.saturating_sub(1) as usize) * AP_PAGE_SIZE;
        let items = fetch_items(offset as u32, AP_PAGE_SIZE).await?;
        let has_next = offset + items.len() < total;
        let mut obj = serde_json::json!({
            "@context": AP_CONTEXT,
            "type": "OrderedCollectionPage",
            "id": format!("{}?page={}", collection_url, page_number),
            "partOf": collection_url,
            "totalItems": total,
            "orderedItems": items,
        });
        if has_next {
            obj["next"] = serde_json::json!(format!("{}?page={}", collection_url, page_number + 1));
        }
        obj
    } else {
        serde_json::json!({
            "@context": AP_CONTEXT,
            "type": "OrderedCollection",
            "id": collection_url,
            "totalItems": total,
            "first": format!("{}?page=1", collection_url),
        })
    };
    Ok(serde_json::to_string(&json)?)
}
