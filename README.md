# k-ap

Generic ActivityPub protocol layer for Rust services. Extracted from the `thoughts` and `movies-diary` projects.

Wraps [`activitypub_federation`](https://crates.io/crates/activitypub_federation) and provides the plumbing that every AP-enabled service needs: actor management, inbox/outbox routing, follower tracking, WebFinger, NodeInfo, and HTTP signature handling.

Not domain-specific — no opinions about what your content type looks like.

## Add as dependency

```toml
[dependencies]
k-ap = { git = "https://git.gabrielkaszewski.dev/GKaszewski/k-ap.git", tag = "v0.1.0" }
```

## What you implement

Three traits wire your data layer into `k-ap`:

```rust
// Your database layer for follows, keypairs, remote actors, blocks
impl FederationRepository for MyFederationRepo { ... }

// Your user lookup (id, username, bio, avatar)
impl ApUserRepository for MyUserRepo { ... }

// Dispatch incoming AP objects to the right handler
impl ApObjectHandler for MyObjectHandler { ... }
```

## Wire up the service

```rust
use k_ap::{ActivityPubService, FederationRepository, ApUserRepository, ApObjectHandler};

let service = ActivityPubService::builder(
    Arc::new(my_federation_repo),
    Arc::new(my_user_repo),
    Arc::new(my_object_handler),
    "https://example.com",
)
.allow_registration(true)
.software_name("my-app")
.build()
.await?;

// Mount the AP routes onto your axum router
let router = Router::new().merge(service.router());
```

## What the service handles for you

- **Actor** — `GET /users/:id` serves the AP Person object with public key
- **Inbox** — `POST /users/:id/inbox` + `POST /inbox` (shared), verifies HTTP signatures, dispatches to your `ApObjectHandler`
- **Outbox** — `GET /users/:id/outbox` with OrderedCollection pagination
- **Followers / Following** — `GET /users/:id/followers` and `/following`
- **WebFinger** — `GET /.well-known/webfinger`
- **NodeInfo** — `GET /.well-known/nodeinfo` + `GET /nodeinfo/2.1`

## Broadcast from your domain layer

```rust
// Fan out a new note to all accepted followers
service.broadcast_create_note(user_id, &note_json).await?;
service.broadcast_update_note(user_id, &note_json).await?;

// Announce / Undo Announce
service.broadcast_announce_to_followers(user_id, object_ap_id).await?;
service.broadcast_undo_announce_to_followers(user_id, object_ap_id, object_url).await?;

// Like / Unlike to a remote inbox
service.broadcast_like_to_inbox(user_id, object_ap_id, inbox_url).await?;
service.broadcast_undo_like_to_inbox(user_id, object_ap_id, inbox_url).await?;

// Follow / Unfollow / Accept / Reject
service.follow(local_user_id, remote_actor_url, handle).await?;
service.unfollow(local_user_id, remote_actor_url).await?;
service.accept_follower(local_user_id, remote_actor_url).await?;
service.reject_follower(local_user_id, remote_actor_url).await?;
```

## Project-specific ports

`k-ap` does not define port traits tied to your domain (e.g. `OutboundFederationPort`, `ActivityPubRepository<Thought>`). Those belong in your adapter layer and are wired up there. See `crates/adapters/activitypub/src/port.rs` in `thoughts` for a reference implementation.

## Key public types

| Type | Description |
|------|-------------|
| `ActivityPubService` | Central service — build once, share via `Arc` |
| `FederationData` | Request-scoped data passed through the federation layer |
| `FederationRepository` | Trait: follows, keypairs, remote actors, blocks |
| `ApUserRepository` | Trait: user lookup by id / username |
| `ApObjectHandler` | Trait: dispatch incoming AP objects |
| `RemoteActor` | A federated actor record |
| `Follower` / `FollowerStatus` | Follower with pending/accepted/rejected state |
| `ApUser` | AP-serializable local user |
| `ApFederationConfig` | Wraps the `activitypub_federation` config |
| `Error` | AP-layer error type |
