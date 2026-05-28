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

Seven focused traits wire your data layer into `k-ap`. You can implement them all on a single database struct by cloning the `Arc`.

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

// Read side — provides local content to the library (outbox, backfill)
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

- **Actor** — `GET /users/:id` serves the AP Person object with public key and security vocabulary `@context`
- **Inbox** — `POST /users/:id/inbox` + `POST /inbox` (shared), verifies HTTP signatures, dispatches to your `ApObjectHandler`. 1 MB body limit enforced.
- **Outbox** — `GET /users/:id/outbox` with cursor-based `OrderedCollection` pagination
- **Followers / Following** — `GET /users/:id/followers` and `/following`
- **WebFinger** — `GET /.well-known/webfinger` with `aliases` field
- **NodeInfo** — `GET /.well-known/nodeinfo` + `GET /nodeinfo/2.0`

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
    also_known_as: Option<String>,        // for account migration
    profile_url: Option<Url>,
    featured_url: Option<Url>,            // pinned posts collection
    attachment: Vec<ApProfileField>,      // profile metadata fields
    manually_approves_followers: bool,    // controls manuallyApprovesFollowers in AP JSON
    discoverable: bool,                   // controls discoverable in AP JSON
    actor_type: ApActorType,              // Person / Service / Application / Organization / Group
}
```

## Broadcast with visibility

```rust
use k_ap::ApVisibility;

// Public — to: [AS_PUBLIC], cc: [followers]
service.broadcast_create_note(user_id, note_json, ApVisibility::Public).await?;

// Followers only — to: [followers], cc: []
service.broadcast_create_note(user_id, note_json, ApVisibility::FollowersOnly).await?;

// Private — no delivery, library returns immediately
service.broadcast_create_note(user_id, note_json, ApVisibility::Private).await?;

service.broadcast_update_note(user_id, note_json, ApVisibility::Public).await?;
service.broadcast_delete_to_followers(user_id, ap_id).await?;

// Announce / Undo Announce
service.broadcast_announce_to_followers(user_id, object_ap_id).await?;
service.broadcast_undo_announce_to_followers(user_id, object_ap_id).await?;

// Like / Unlike to a remote inbox
service.broadcast_like_to_inbox(user_id, object_ap_id, inbox_url).await?;
service.broadcast_undo_like_to_inbox(user_id, object_ap_id, inbox_url).await?;

// Actor profile update
service.broadcast_actor_update(user_id).await?;

// Account migration — sends a Move activity to all followers
// Pre-condition: set alsoKnownAs on the local actor before calling this
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
// Use these when delivering Accept/Reject from a separate worker process
service.mark_follower_accepted(local_user_id, remote_actor_url).await?;
service.mark_follower_rejected(local_user_id, remote_actor_url).await?;
```

## Async delivery via EventPublisher

By default, outbound activities are sent via `tokio::spawn` (fire-and-forget). Implement `EventPublisher` to route deliveries through your job queue instead:

```rust
impl EventPublisher for MyQueue {
    async fn publish(&self, event: FederationEvent) -> anyhow::Result<()> {
        match event {
            FederationEvent::DeliveryRequested { inbox, activity, signing_actor_id } => {
                // Persist and enqueue the delivery task
                self.enqueue(inbox, activity, signing_actor_id).await?;
            }
            FederationEvent::DeliveryFailed { inbox, error, .. } => {
                // Log or alert on permanent failure
            }
        }
        Ok(())
    }
}

// When your worker processes the queue item:
service.deliver_to_inbox(inbox, activity_json, signing_actor_id).await?;
```

## Actor lookup

```rust
// Resolve a handle via WebFinger using a signed HTTP request.
// Works with strict instances (e.g. Threads) that require HTTP signatures.
let actor: LookedUpActor = service.lookup_actor_by_handle("@user@remote.example").await?;
```

## Inbound activity handling

The library handles the following inbound AP activities out of the box:

`Follow`, `Accept`, `Reject`, `Undo` (Follow, Like, Announce, Add), `Create`, `Update`, `Delete`, `Announce`, `Like`, `Add`, `Block`, `Move`

All activities are deduplicated by activity `id` — safe against retried deliveries.

Mentions are extracted from `tag` arrays in `Create`/`Update` and dispatched via `ApObjectHandler::on_mention`.

`Move` is fully handled: verifies `alsoKnownAs` cross-reference on the target, migrates all local follower records, and re-follows the new actor on behalf of affected users.

Actor types accepted on inbound: `Person`, `Service`, `Application`, `Organization`, `Group`.

## Key public types

| Type | Description |
|------|-------------|
| `ActivityPubService` | Central service — build once, share via `Arc` |
| `ActivityRepository` | Trait: activity deduplication |
| `FollowRepository` | Trait: follower/following graph + migration |
| `ActorRepository` | Trait: keypairs, remote actor cache, announce tracking |
| `BlocklistRepository` | Trait: domain and actor blocklists |
| `ApUserRepository` | Trait: user lookup |
| `ApContentReader` | Trait: provides local content for outbox/backfill |
| `ApObjectHandler` | Trait: dispatches inbound AP activities |
| `ApVisibility` | `Public` / `FollowersOnly` / `Private` |
| `ApActorType` | `Person` / `Service` / `Application` / `Organization` / `Group` |
| `FederationEvent` | `DeliveryRequested` / `DeliveryFailed` — for job queue integration |
| `EventPublisher` | Trait: hook for async delivery via external queue |
| `LookedUpActor` | Resolved remote actor from `lookup_actor_by_handle` |
| `RemoteActor` | A cached federated actor record |
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
