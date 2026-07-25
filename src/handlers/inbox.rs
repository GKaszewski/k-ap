use activitypub_federation::{
    axum::inbox::{ActivityData, receive_activity},
    config::Data,
    protocol::context::WithContext,
};

use crate::activities::InboxActivities;
use crate::actors::DbActor;
use crate::data::FederationData;
use crate::error::Error;

pub async fn inbox_handler(
    data: Data<FederationData>,
    activity_data: ActivityData,
) -> Result<(), Error> {
    let result = receive_activity::<WithContext<InboxActivities>, DbActor, FederationData>(
        activity_data,
        &data,
    )
    .await;

    match result {
        Ok(()) => Ok(()),
        Err(Error::Internal(ref inner)) if is_unknown_activity_error(inner) => {
            tracing::debug!(error = %inner, "unknown activity type, accepted without processing");
            Ok(())
        }
        Err(error) => Err(error),
    }
}

fn is_unknown_activity_error(error: &anyhow::Error) -> bool {
    let message = error.to_string();
    message.contains("unknown variant") || message.contains("does not match any variant")
}
