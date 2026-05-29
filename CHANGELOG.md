# Changelog

## [0.3.0] — unreleased

### Breaking changes

**Builder API** — the service builder no longer takes positional arguments. All repos are named setters:

```rust
// Before (0.2.x)
ActivityPubService::builder(repo, user_repo, handler, "https://example.com")

// After (0.3.0)
ActivityPubService::builder("https://example.com")
    .activity_repo(db.clone())
    .follow_repo(db.clone())
    .actor_repo(db.clone())
    .blocklist_repo(db.clone())
    .user_repo(db.clone())
    .content_reader(db.clone())
    .object_handler(db.clone())
    .build()
    .await?
```

**`FederationRepository` split into 4 focused traits** — implement each independently or all on one struct:

| Old | New |
|-----|-----|
| `FederationRepository` (34 methods) | `ActivityRepository` (2) |
| | `FollowRepository` (18) |
| | `ActorRepository` (6) |
| | `BlocklistRepository` (8) |

**`ApObjectHandler` split into read/write traits:**

| Old | New |
|-----|-----|
| `ApObjectHandler::get_local_objects_for_user` | removed (dead code) |
| `ApObjectHandler::get_local_objects_page` | `ApContentReader::get_local_objects_page` |
| `ApObjectHandler::count_local_posts` | `ApContentReader::count_local_posts` |
| `ApObjectHandler` (callbacks) | `ApObjectHandler` (9 callbacks, unchanged) |

`ApContentReader` also has a new optional method with a default empty impl:
- `get_featured_objects(user_id) -> Vec<Url>` — override to expose pinned posts

**`broadcast_create_note` and `broadcast_update_note` — new `mentioned_inboxes` parameter:**

```rust
// Before
service.broadcast_create_note(user_id, note_json, ApVisibility::Public).await?;

// After — pass inboxes of mentioned non-followers, or vec![] if none
service.broadcast_create_note(user_id, note_json, ApVisibility::Public, mentioned_inboxes).await?;
```

**`ApUser::also_known_as` changed from `Option<String>` to `Vec<String>`** — stores all aliases, not just the first.

**`LookedUpActor::also_known_as` changed from `Option<String>` to `Vec<String>`.**

**`backfill_outbox` renamed to `import_remote_outbox`** — clarifies direction: imports content FROM a remote server into your instance, distinct from `run_backfill_for_follower` which sends your content TO a new follower.

**`FollowRepository` — two new required methods:**
- `count_accepted_followers(user_id) -> usize` — DB-side count, replaces loading all followers into memory
- `get_accepted_followers_page(user_id, offset, limit) -> Vec<RemoteActor>` — DB-side paginated listing

**`ActorRepository` — one new required method:**
- `remove_announce(activity_id, actor_url)` — called when `Undo(Announce)` is received

---

### New features

**`ApVisibility`** — controls `to`/`cc` addressing and delivery scope for Create/Update:
- `Public` — `to: [AS_PUBLIC], cc: [followers]`
- `FollowersOnly` — `to: [followers], cc: []`
- `Private` — no delivery at all

**Mention delivery** — `broadcast_create_note` and `broadcast_update_note` accept `mentioned_inboxes: Vec<Url>`. Delivery goes to followers + mentioned non-followers, deduplicated.

**`ApUser::discoverable: bool`** — serialized as `discoverable` in actor JSON (was hard-coded `true`).

**`ApUser::actor_type: ApActorType`** — serialized as the AP actor type (was hard-coded `Person`).

**`ApUser::featured_url: Option<Url>`** — serialized as `featured` in actor JSON. The library serves `GET /users/{id}/featured` automatically.

**`GET /users/{id}/featured` route** — serves an `OrderedCollection` of pinned posts via `ApContentReader::get_featured_objects`. Default is an empty collection.

**`router()` no longer registers `GET /users/{id}`, `/followers`, or `/following`** — these paths need content negotiation (AP JSON vs UI JSON) which k-ap can't do. Your application owns those routes and calls `actor_json`, `followers_collection_json`, `following_collection_json` to produce the AP response. See README for the pattern.

**`FederationEvent::BackfillRequested`** — when an `EventPublisher` is configured, `accept_follower` publishes this event instead of spawning an in-process task. Process it by calling `run_backfill_for_follower`.

**`run_backfill_for_follower(owner_user_id, follower_inbox_url)`** — public method for workers processing `BackfillRequested` events.

**`ApObjectHandler::on_announce_removed`** — called when `Undo(Announce)` is received for a locally-authored object. Default is a no-op — override to update boost counts.

**`ApObjectHandler::on_announce_of_remote`** — called when a remote actor boosts a non-local object. Default is a no-op.

**`count_accepted_followers` / `get_accepted_followers_page`** — service methods now use DB-side queries via the new `FollowRepository` methods instead of loading all followers into memory.

---

### Bug fixes

**`AddActivity` used activity ID instead of object ID** — `on_create` was receiving the Add activity's `id` instead of `object["id"]`. Now matches `CreateActivity` behaviour.

**`Undo(Announce)` was silently ignored** — now removes the announce record from `ActorRepository` and calls `on_announce_removed`. Announce counts no longer drift.

**`Move` re-follows blocked the inbox handler** — re-follow HTTP requests are now spawned in the background so the inbox handler returns immediately.

**`alsoKnownAs` truncated to first alias** — `from_json` now stores all aliases from the incoming actor JSON.

**`Move` alsoKnownAs check now verifies all aliases** — previously only checked the first one.

**Block check before actor HTTP fetch** — `FollowActivity::receive` now checks per-actor blocks before calling `dereference()`, preventing SSRF from blocked actors.

---

### Other improvements

- 1 MB `DefaultBodyLimit` on inbox routes — prevents memory exhaustion from oversized payloads
- 4xx error responses now use generic messages — internal details are only logged, never sent to clients
- Actor JSON includes W3C security vocabulary in `@context` — required for public key resolution by strict implementations
- WebFinger response includes `aliases` field
- Outbox `last` link added to root `OrderedCollection`
- Outbox count uses `count_local_posts()` — no longer loads all objects to count them
- Backfill uses cursor-based `get_local_objects_page` — no longer loads all posts into memory
- All inbound activities are deduplicated by activity `id` via `ActivityRepository`
- Mentions extracted from `tag` arrays in `Create`/`Update` and dispatched via `on_mention`
- Cross-server `Announce` dispatched via `on_announce_of_remote` instead of silently dropped
- `discoverable`, `actor_type`, `featured_url`, `manually_approves_followers` all configurable per-user
- `delivery_max_attempts` and `delivery_initial_delay_secs` configurable via builder
- `Makefile` with `check`, `fmt`, `clippy`, `test`, `fix` targets

---

## [0.1.10] — 2024 (previous release)

Initial public extraction from `thoughts` and `movies-diary`.

- `FederationRepository`, `ApUserRepository`, `ApObjectHandler` — single-struct trait interface
- Inbound: Follow, Accept, Reject, Undo, Create, Update, Delete, Announce, Like, Add, Block, Move
- Outbound broadcasts: Create, Update, Delete, Announce, Like, Move, actor update
- WebFinger, NodeInfo 2.0, shared inbox, follower/following collections
- Signed WebFinger resolution for actor lookup
- Account migration (Move) with alsoKnownAs verification and re-follow
