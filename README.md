# k-ap

Generic ActivityPub protocol layer for Rust services. Extracted from the `thoughts` and `movies-diary` projects.

Wraps [`activitypub_federation`](https://crates.io/crates/activitypub_federation) and provides the plumbing that every AP-enabled service needs: actor management, inbox/outbox routing, follower tracking, WebFinger, NodeInfo, and HTTP signature handling.

Not domain-specific — no opinions about what your content type looks like.

## Add as dependency

```toml
[dependencies]
k-ap = { git = "https://git.gabrielkaszewski.dev/GKaszewski/k-ap.git", tag = "v0.3.0" }
```

## What you implement

Seven focused traits wire your data layer into `k-ap`. Implement them all on a single database struct by cloning the `Arc`, or use separate structs for different backends.

```rust
// Activity deduplication — idempotency for inbound deliveries
impl ActivityRepository for MyDb { ... }

// Follower / following graph + account migration
impl FollowRepository for MyDb { ... }

// Local keypairs, remote actor cache, boost (Announce) tracking
impl ActorRepository for MyDb { ... }

// Domain and per-user actor blocklists
impl BlocklistRepository for MyDb { ... }

// User lookup by id / username
impl ApUserRepository for MyDb { ... }

// Read side — provides local content to the library (outbox, backfill, featured)
impl ApContentReader for MyDb { ... }

// Write side — called when the inbox receives AP activities
impl ApObjectHandler for MyDb { ... }
```

## Wire up the service

```rust
use std::sync::Arc;
use k_ap::ActivityPubService;

let db = Arc::new(MyDb::new(...));

let service = ActivityPubService::builder("https://example.com")
    .activity_repo(db.clone())
    .follow_repo(db.clone())
    .actor_repo(db.clone())
    .blocklist_repo(db.clone())
    .user_repo(db.clone())
    .content_reader(db.clone())
    .object_handler(db.clone())
    .allow_registration(false)
    .software_name("my-app")
    .build()
    .await?;

// Mount the AP routes onto your axum router
let router = Router::new().merge(service.router());
```

## What the service handles for you

| Route | Description |
|-------|-------------|
| `GET /users/{id}` | AP actor JSON with public key and security `@context` |
| `POST /users/{id}/inbox` | Per-user inbox — verifies HTTP signatures, 1 MB limit |
| `POST /inbox` | Shared inbox — same verification |
| `GET /users/{id}/outbox` | Cursor-based `OrderedCollection` |
| `GET /users/{id}/followers` | Offset-paginated follower collection |
| `GET /users/{id}/following` | Offset-paginated following collection |
| `GET /users/{id}/featured` | Pinned posts `OrderedCollection` (from `get_featured_objects`) |
| `GET /.well-known/webfinger` | JRD with `aliases` field |
| `GET /.well-known/nodeinfo` | NodeInfo well-known redirect |
| `GET /nodeinfo/2.0` | NodeInfo 2.0 |

## ApUser fields

Your `ApUserRepository` returns `ApUser`. All fields control how the actor is serialized:

```rust
ApUser {
    id: uuid,
    username: String,
    display_name: Option<String>,
    bio: Option<String>,
    avatar_url: Option<Url>,
    banner_url: Option<Url>,
    also_known_as: Vec<String>,           // all known aliases, for account migration
    profile_url: Option<Url>,
    featured_url: Option<Url>,            // pinned posts collection URL
    attachment: Vec<ApProfileField>,      // profile metadata (PropertyValue)
    manually_approves_followers: bool,    // controls manuallyApprovesFollowers in AP JSON
    discoverable: bool,                   // controls discoverable in AP JSON
    actor_type: ApActorType,              // Person / Service / Application / Organization / Group
}
```

## Broadcast with visibility

```rust
use k_ap::ApVisibility;

// Resolve the inboxes of any mentioned non-followers first, then pass them in.
// The library delivers to followers + mentioned actors, deduplicated.
let mentioned = vec![
    service.lookup_actor_by_handle("@bob@mastodon.social").await?.outbox_url.unwrap(),
    // ...or resolve inbox URLs directly
];

// Public — to: [AS_PUBLIC], cc: [followers]; delivered to followers + mentioned
service.broadcast_create_note(user_id, note_json, ApVisibility::Public, mentioned).await?;

// Followers only — to: [followers], cc: []; delivered to followers + mentioned
service.broadcast_create_note(user_id, note_json, ApVisibility::FollowersOnly, vec![]).await?;

// Private — no delivery at all; library returns immediately
service.broadcast_create_note(user_id, note_json, ApVisibility::Private, vec![]).await?;

service.broadcast_update_note(user_id, note_json, ApVisibility::Public, vec![]).await?;
service.broadcast_delete_to_followers(user_id, ap_id).await?;

// Announce / Undo Announce
service.broadcast_announce_to_followers(user_id, object_ap_id).await?;
service.broadcast_undo_announce_to_followers(user_id, object_ap_id).await?;

// Like / Unlike to a single remote inbox
service.broadcast_like_to_inbox(user_id, object_ap_id, inbox_url).await?;
service.broadcast_undo_like_to_inbox(user_id, object_ap_id, inbox_url).await?;

// Actor profile update to all followers
service.broadcast_actor_update(user_id).await?;

// Account migration — sends Move to all followers; set alsoKnownAs first
service.broadcast_move(user_id, new_actor_url).await?;
```

## Follow management

```rust
// Outbound follows (resolves handle via signed WebFinger request)
service.follow(local_user_id, "@user@remote.example").await?;
service.unfollow(local_user_id, remote_actor_url).await?;

// Inbound follow requests — full flow (DB update + AP delivery + backfill)
service.accept_follower(local_user_id, remote_actor_url).await?;
service.reject_follower(local_user_id, remote_actor_url).await?;

// Inbound follow requests — DB only (no AP delivery)
// Use when delivering Accept/Reject from a separate worker process
service.mark_follower_accepted(local_user_id, remote_actor_url).await?;
service.mark_follower_rejected(local_user_id, remote_actor_url).await?;

// Querying followers (DB-side filtering — efficient for large accounts)
let count: usize = service.count_accepted_followers(user_id).await?;
let page: Vec<RemoteActor> = service.get_accepted_followers_page(user_id, 0, 20).await?;
```

## Async delivery and backfill via EventPublisher

By default, outbound delivery and backfill run in the same process via `tokio::spawn`.
Implement `EventPublisher` to route them through your job queue so workers can process them separately:

```rust
impl EventPublisher for MyQueue {
    async fn publish(&self, event: FederationEvent) -> anyhow::Result<()> {
        match event {
            FederationEvent::DeliveryRequested { inbox, activity, signing_actor_id } => {
                // Persist and enqueue; your worker calls deliver_to_inbox
                self.enqueue_delivery(inbox, activity, signing_actor_id).await?;
            }
            FederationEvent::DeliveryFailed { inbox, error, .. } => {
                tracing::error!(%inbox, %error, "permanent delivery failure");
            }
            FederationEvent::BackfillRequested { owner_user_id, follower_inbox_url } => {
                // Enqueue; your worker calls run_backfill_for_follower
                self.enqueue_backfill(owner_user_id, follower_inbox_url).await?;
            }
        }
        Ok(())
    }
}

// Worker: execute a delivery task from the queue
service.deliver_to_inbox(inbox, activity_json, signing_actor_id).await?;

// Worker: send local content to a new follower's inbox
service.run_backfill_for_follower(owner_user_id, follower_inbox_url).await?;
```

## Remote outbox import

Import a remote actor's post history (e.g. after a local user follows them):

```rust
// Fetches pages from outbox_url and calls ApObjectHandler::on_create for each.
// Distinct from run_backfill_for_follower which sends YOUR content TO a follower.
service.import_remote_outbox(outbox_url, actor_url).await?;
```

## Actor lookup

```rust
// Resolve a handle via WebFinger using a signed HTTP request.
// Works with strict instances (e.g. Threads) that require HTTP signatures.
let actor: LookedUpActor = service.lookup_actor_by_handle("@user@remote.example").await?;
```

## Pinned posts (featured collection)

Override `get_featured_objects` in your `ApContentReader` to expose pinned posts.
The library serves them at `GET /users/{id}/featured` automatically. Default is empty.

```rust
impl ApContentReader for MyDb {
    async fn get_featured_objects(&self, user_id: uuid::Uuid) -> anyhow::Result<Vec<Url>> {
        Ok(self.get_pinned_post_urls(user_id).await?)
    }
    // ...other methods
}
```

Set `featured_url` in `ApUser` to point to the endpoint — the library includes it in actor JSON:

```rust
ApUser {
    featured_url: Some("https://example.com/users/{id}/featured".parse()?),
    // ...
}
```

## Inbound activity handling

Handled out of the box:

`Follow`, `Accept`, `Reject`, `Undo` (Follow, Like, Announce, Add, Block), `Create`, `Update`, `Delete`, `Announce`, `Like`, `Add`, `Block`, `Move`

- All activities are deduplicated by `id` — safe against retried deliveries.
- Mentions are extracted from `tag` arrays and dispatched via `ApObjectHandler::on_mention`.
- `Undo(Announce)` removes the boost record from `ActorRepository` and calls `on_announce_removed`.
- `Move` verifies all `alsoKnownAs` aliases on the target, migrates follower records, and re-follows in a background task (non-blocking).
- Actor types accepted: `Person`, `Service`, `Application`, `Organization`, `Group`.

## Key public types

| Type | Description |
|------|-------------|
| `ActivityPubService` | Central service — build once, share via `Arc` |
| `ActivityRepository` | Trait: activity ID deduplication (2 methods) |
| `FollowRepository` | Trait: follower/following graph + migration (18 methods) |
| `ActorRepository` | Trait: keypairs, remote actor cache, announce tracking (6 methods) |
| `BlocklistRepository` | Trait: domain and actor blocklists (8 methods) |
| `ApUserRepository` | Trait: user lookup (3 methods) |
| `ApContentReader` | Trait: outbox/backfill/featured content (3 methods, 1 with default) |
| `ApObjectHandler` | Trait: inbound activity callbacks (9 methods, 2 with defaults) |
| `ApVisibility` | `Public` / `FollowersOnly` / `Private` |
| `ApActorType` | `Person` / `Service` / `Application` / `Organization` / `Group` |
| `FederationEvent` | `DeliveryRequested` / `DeliveryFailed` / `BackfillRequested` |
| `EventPublisher` | Trait: hook for job queue integration |
| `LookedUpActor` | Resolved remote actor from `lookup_actor_by_handle` |
| `RemoteActor` | Cached federated actor record |
| `Follower` / `FollowerStatus` | Follower with `Pending`/`Accepted`/`Rejected` state |
| `ApUser` | AP-serializable local user |
| `ApFederationConfig` | Wraps the `activitypub_federation` config |
| `Error` | AP-layer error type |

## Local development

```bash
make check    # fmt --check + clippy -D warnings + tests (use before committing)
make fmt      # apply rustfmt
make fix      # fmt + clippy --fix
```
