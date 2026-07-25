use url::Url;

use crate::user::ApVisibility;

pub(crate) struct Addressing {
    pub to: Vec<String>,
    pub cc: Vec<String>,
}

pub(crate) fn visibility_addressing(visibility: ApVisibility, followers_url: &Url) -> Addressing {
    match visibility {
        ApVisibility::Public => Addressing {
            to: vec![crate::urls::AS_PUBLIC.to_string()],
            cc: vec![followers_url.to_string()],
        },
        ApVisibility::FollowersOnly => Addressing {
            to: vec![followers_url.to_string()],
            cc: vec![],
        },
        ApVisibility::Private => Addressing {
            to: vec![],
            cc: vec![],
        },
    }
}

#[derive(serde::Serialize)]
pub(super) struct AnnounceRef {
    #[serde(rename = "type")]
    pub kind: &'static str,
    pub id: String,
    pub actor: String,
    pub object: String,
}

#[derive(serde::Serialize)]
pub(super) struct LikeRef {
    #[serde(rename = "type")]
    pub kind: &'static str,
    pub id: String,
    pub actor: String,
    pub object: String,
}

#[derive(serde::Serialize)]
pub(super) struct TombstoneRef {
    #[serde(rename = "type")]
    pub kind: &'static str,
    pub id: String,
}

#[derive(serde::Serialize)]
pub(super) struct AddRef {
    #[serde(rename = "type")]
    pub kind: &'static str,
    pub id: String,
    pub object: AddRefObject,
}

#[derive(serde::Serialize)]
pub(super) struct AddRefObject {
    pub id: String,
}
