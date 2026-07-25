mod accept;
mod add;
pub(crate) mod announce;
pub(crate) mod block;
mod create;
mod delete;
mod follow;
pub(crate) mod helpers;
pub(crate) mod like;
mod move_act;
mod reject;
mod undo;
mod update;

pub use accept::AcceptActivity;
pub use add::AddActivity;
pub use announce::AnnounceActivity;
pub use block::BlockActivity;
pub use create::CreateActivity;
pub use delete::DeleteActivity;
pub use follow::FollowActivity;
pub use like::LikeActivity;
pub use move_act::MoveActivity;
pub use reject::RejectActivity;
pub use undo::UndoActivity;
pub use update::UpdateActivity;

use activitypub_federation::config::Data;
use serde::{Deserialize, Serialize};
use url::Url;

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type")]
#[enum_delegate::implement(activitypub_federation::traits::Activity)]
pub enum InboxActivities {
    #[serde(rename = "Follow")]
    Follow(FollowActivity),
    #[serde(rename = "Accept")]
    Accept(AcceptActivity),
    #[serde(rename = "Reject")]
    Reject(RejectActivity),
    #[serde(rename = "Undo")]
    Undo(UndoActivity),
    #[serde(rename = "Create")]
    Create(CreateActivity),
    #[serde(rename = "Delete")]
    Delete(DeleteActivity),
    #[serde(rename = "Update")]
    Update(UpdateActivity),
    #[serde(rename = "Announce")]
    Announce(AnnounceActivity),
    #[serde(rename = "Add")]
    Add(AddActivity),
    #[serde(rename = "Block")]
    Block(BlockActivity),
    #[serde(rename = "Like")]
    Like(LikeActivity),
    #[serde(rename = "Move")]
    Move(MoveActivity),
}
