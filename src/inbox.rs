use activitypub_federation::{
    axum::inbox::{ActivityData, receive_activity},
    config::Data,
    protocol::context::WithContext,
};

use crate::activities::InboxActivities;
use crate::actors::DbActor;
use crate::data::FederationData;
use crate::error::Error;

/// Idempotency is enforced inside each activity's `receive()` implementation
/// via `FederationRepository::is_activity_processed` /
/// `mark_activity_processed`. HTTP signature verification and JSON-LD
/// processing are handled by `activitypub_federation` middleware before this
/// handler is reached.
pub async fn inbox_handler(
    data: Data<FederationData>,
    activity_data: ActivityData,
) -> Result<(), Error> {
    receive_activity::<WithContext<InboxActivities>, DbActor, FederationData>(activity_data, &data)
        .await
}
