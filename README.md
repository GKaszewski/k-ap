# k-ap

Generic ActivityPub protocol layer for Rust services. Extracted from the `thoughts` and `movies-diary` projects.

Wraps [`activitypub_federation`](https://crates.io/crates/activitypub_federation) and provides the plumbing that every AP-enabled service needs: actor management, inbox/outbox routing, follower tracking, WebFinger, NodeInfo, and HTTP signature handling.

Not domain-specific — no opinions about what your content type looks like.

## Add as dependency

Via the private Gitea registry (recommended):

```toml
[dependencies]
k-ap = { version = "0.5.0", registry = "gitea" }
```

Configure the registry in `.cargo/config.toml`:

```toml
[registries.gitea]
index = "sparse+https://git.gabrielkaszewski.dev/api/packages/GKaszewski/cargo/"
```

Or via git if you don't have registry access:

```toml
[dependencies]
k-ap = { git = "https://git.gabrielkaszewski.dev/GKaszewski/k-ap.git", tag = "v0.5.0" }
```

## What you implement

Seven supertrait facades wire your data layer into `k-ap`. Implement them all on a single database struct by cloning the `Arc`, or use separate structs for different backends.

```rust
// Activity deduplication — idempotency for inbound deliveries
impl ActivityRepository for MyDb { ... }

// Follower / following graph + account migration (5 sub-traits)
impl FollowRepository for MyDb { ... }

// Local keypairs, remote actor cache, boost (Announce) tracking (3 sub-traits)
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

`FollowRepository` and `ActorRepository` are composed from fine-grained sub-traits (`FollowerWriter`, `FollowerReader`, `FollowingWriter`, `FollowingReader`, `FollowMigration`, `KeypairRepository`, `RemoteActorCache`, `AnnounceRepository`). Implement the supertrait and Rust auto-derives it, or implement sub-traits individually for more control.

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
    // .url_scheme(Arc::new(MyScheme))  // optional: customize URL patterns
    .build()
    .await?;

// Mount the AP routes onto your axum router
let router = Router::new().merge(service.router());
```

## What the service handles for you

`service.router()` registers only the routes k-ap fully owns:

| Route | Description |
|-------|-------------|
| `POST /inbox` | Shared inbox — HTTP signature verification + dispatch, 1 MB limit |
| `POST /users/{id}/inbox` | Per-user inbox — same |
| `GET /users/{id}/outbox` | Cursor-based `OrderedCollection` |
| `GET /users/{id}/featured` | Pinned posts `OrderedCollection` |
| `GET /.well-known/webfinger` | JRD with `aliases` field |
| `GET /.well-known/nodeinfo` | NodeInfo well-known redirect |
| `GET /nodeinfo/2.0` | NodeInfo 2.0 |

**Not registered by `router()`:** `GET /users/{id}`, `GET /users/{id}/followers`, `GET /users/{id}/following`.

These paths are dual-purpose in real applications — they must serve both AP JSON (`application/activity+json`) and the app's own UI JSON (content negotiation). k-ap can't do the UI half, so your application owns the route and calls k-ap's helper methods to produce the AP response:

```rust
// In your axum actor handler — serve AP JSON or UI JSON based on Accept header
async fn actor_handler(Path(username): Path<String>, headers: HeaderMap, ...) {
    if wants_ap_json(&headers) {
        let json = service.actor_json(&user.id.to_string()).await?;
        return ap_json_response(json);
    }
    // ... serve UI response
}

// Similarly for followers/following:
let json = service.followers_collection_json(user_id, page).await?;
let json = service.following_collection_json(user_id, page).await?;
```

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
service.broadcast_create(user_id, note_json, ApVisibility::Public, mentioned).await?;

// Followers only — to: [followers], cc: []; delivered to followers + mentioned
service.broadcast_create(user_id, note_json, ApVisibility::FollowersOnly, vec![]).await?;

// Private — no delivery at all; library returns immediately
service.broadcast_create(user_id, note_json, ApVisibility::Private, vec![]).await?;

service.broadcast_update(user_id, note_json, ApVisibility::Public, vec![]).await?;
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

// Send arbitrary AP activity JSON to all followers (escape hatch for custom types)
service.broadcast_raw_to_followers(user_id, activity_json).await?;
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
| `FollowRepository` | Supertrait: follower/following graph + migration (19 methods via 5 sub-traits) |
| &emsp;`FollowerWriter` | Sub-trait: add/remove/update follower records (4 methods) |
| &emsp;`FollowerReader` | Sub-trait: query followers, counts, pages (7 methods) |
| &emsp;`FollowingWriter` | Sub-trait: add/remove/update following records (4 methods) |
| &emsp;`FollowingReader` | Sub-trait: query following, counts, pages (3 methods) |
| &emsp;`FollowMigration` | Sub-trait: account migration (1 method, has default no-op) |
| `ActorRepository` | Supertrait: keypairs, remote actor cache, announce tracking (7 methods via 3 sub-traits) |
| &emsp;`KeypairRepository` | Sub-trait: local actor keypair storage (2 methods) |
| &emsp;`RemoteActorCache` | Sub-trait: remote actor upsert/lookup (2 methods) |
| &emsp;`AnnounceRepository` | Sub-trait: boost tracking (3 methods) |
| `BlocklistRepository` | Trait: domain and actor blocklists (8 methods) |
| `ApUserRepository` | Trait: user lookup (3 methods) |
| `ApContentReader` | Trait: outbox/backfill/featured content (3 methods, 1 with default) |
| `ApObjectHandler` | Trait: inbound activity callbacks (11 methods, 2 with defaults) |
| `UrlScheme` / `DefaultUrlScheme` | Trait + default impl: configurable URL patterns for actors and activities |
| `ApVisibility` | `Public` / `FollowersOnly` / `Private` |
| `ApActorType` | `Person` / `Service` / `Application` / `Organization` / `Group` |
| `LocalObject` | Outbox object with addressing fields (`to`, `cc`, `bto`, `bcc`) |
| `Keypair` | Named struct for actor signing keypair |
| `FederationEvent` | `DeliveryRequested` / `DeliveryFailed` / `BackfillRequested` / `OutboundFollowAccepted` |
| `EventPublisher` | Trait: hook for job queue integration |
| `LookedUpActor` | Resolved remote actor from `lookup_actor_by_handle` |
| `RemoteActor` | Cached federated actor record |
| `Follower` / `FollowerStatus` | Follower with `Pending`/`Accepted`/`Rejected` state |
| `ApUser` | AP-serializable local user |
| `ApFederationConfig` | Wraps the `activitypub_federation` config |
| `Error` | thiserror enum: `NotFound` / `BadRequest` / `Unauthorized` / `Forbidden` / `Internal` |

## Custom URL schemes

By default k-ap uses `/users/{uuid}` paths. Implement the `UrlScheme` trait to use custom patterns:

```rust
use k_ap::{UrlScheme, DefaultUrlScheme};
use std::sync::Arc;

struct MyScheme;

impl UrlScheme for MyScheme {
    fn actor_url(&self, base_url: &str, user_id: uuid::Uuid) -> anyhow::Result<Url> {
        // e.g. https://example.com/@username instead of /users/{uuid}
        todo!()
    }
    fn inbox_url(&self, actor_url: &Url) -> anyhow::Result<Url> { todo!() }
    fn shared_inbox_url(&self, base_url: &str) -> Option<Url> { todo!() }
    fn outbox_url(&self, actor_url: &Url) -> anyhow::Result<Url> { todo!() }
    fn followers_url(&self, actor_url: &Url) -> anyhow::Result<Url> { todo!() }
    fn following_url(&self, actor_url: &Url) -> anyhow::Result<Url> { todo!() }
    fn activity_url(&self, base_url: &str) -> anyhow::Result<Url> { todo!() }
    fn extract_user_id(&self, url: &Url) -> Option<uuid::Uuid> { todo!() }
}

let service = ActivityPubService::builder("https://example.com")
    .url_scheme(Arc::new(MyScheme))
    // ...repos...
    .build()
    .await?;
```

## Testing

k-ap ships mock builders for all traits (not behind `#[cfg(test)]` so downstream crates can use them):

```rust
use k_ap::testing::*;

let follow_repo = MockFollowRepoBuilder::new()
    .on_add_follower(|id, url, status, _| Ok(()))
    .on_count_accepted_followers(|_| Ok(42))
    .build();

let actor_repo = MockActorRepoBuilder::new().build();
let blocklist_repo = MockBlocklistRepoBuilder::new().build();
let activity_repo = MockActivityRepoBuilder::new().build();
let user_repo = MockUserRepoBuilder::new().build();
let content_reader = MockContentReaderBuilder::new().build();
let object_handler = MockObjectHandlerBuilder::new().build();
let event_publisher = MockEventPublisherBuilder::new().build();
```

Every unset method defaults to `Ok(Default::default())`.

## Local development

```bash
make check    # fmt --check + clippy -D warnings + tests (use before committing)
make fmt      # apply rustfmt
make fix      # fmt + clippy --fix
```
