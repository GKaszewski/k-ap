# k-ap

Generic ActivityPub protocol layer for Rust services. Extracted from the `thoughts` and `movies-diary` projects.

Wraps [`activitypub_federation`](https://crates.io/crates/activitypub_federation) and provides the plumbing that every AP-enabled service needs: actor management, inbox/outbox routing, follower tracking, WebFinger, NodeInfo, and HTTP signature handling.

Not domain-specific — no opinions about what your content type looks like.

## Add as dependency

```toml
[dependencies]
k-ap = { git = "https://git.gabrielkaszewski.dev/GKaszewski/k-ap.git", tag = "v0.1.10" }
```

## What you implement

Three traits wire your data layer into `k-ap`:

```rust
// Your database layer for follows, keypairs, remote actors, blocks
impl FederationRepository for MyFederationRepo { ... }

// Your user lookup (id, username, bio, avatar, alsoKnownAs)
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
- **NodeInfo** — `GET /.well-known/nodeinfo` + `GET /nodeinfo/2.0`

## Broadcast from your domain layer

```rust
// Fan out a new note to all accepted followers
service.broadcast_create_note(user_id, note_json).await?;
service.broadcast_update_note(user_id, note_json).await?;
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
// Outbound follows (resolves handle via WebFinger)
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

## Actor lookup

```rust
// Resolve a handle via WebFinger using a signed HTTP request.
// Works with strict instances (e.g. Threads) that require HTTP signatures.
let actor: LookedUpActor = service.lookup_actor_by_handle("@user@remote.example").await?;
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
| `LookedUpActor` | Resolved remote actor data from `lookup_actor_by_handle` |
| `RemoteActor` | A federated actor record |
| `Follower` / `FollowerStatus` | Follower with pending/accepted/rejected state |
| `ApUser` | AP-serializable local user (includes `also_known_as`) |
| `ApFederationConfig` | Wraps the `activitypub_federation` config |
| `Error` | AP-layer error type |

## Inbound activity handling

The library handles the following inbound AP activities out of the box:

`Follow`, `Accept`, `Reject`, `Undo` (Follow, Like, Announce), `Create`, `Update`, `Delete`, `Announce`, `Like`, `Add`, `Block`, `Move`

`Move` is fully handled: verifies `alsoKnownAs` cross-reference on the target actor, migrates all local following records, and re-follows the new actor on behalf of affected users.

Actor types accepted: `Person`, `Service`, `Application`, `Organization`, `Group`.
