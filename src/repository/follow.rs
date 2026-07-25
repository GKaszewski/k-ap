use super::{FollowMigration, FollowerReader, FollowerWriter, FollowingReader, FollowingWriter};

/// Manages follower/following relationships and account migration.
pub trait FollowRepository:
    FollowerWriter + FollowerReader + FollowingWriter + FollowingReader + FollowMigration
{
}
impl<T: FollowerWriter + FollowerReader + FollowingWriter + FollowingReader + FollowMigration>
    FollowRepository for T
{
}
