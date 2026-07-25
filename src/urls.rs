pub const AS_PUBLIC: &str = "https://www.w3.org/ns/activitystreams#Public";
pub const AP_CONTEXT: &str = "https://www.w3.org/ns/activitystreams";
pub const AP_CONTENT_TYPE: &str = "application/activity+json";
pub const AP_PAGE_SIZE: usize = 20;
pub const INBOX_BODY_LIMIT: usize = 1024 * 1024;

/// Returns the `@context` array for actor AP JSON.
/// Includes the W3C security vocabulary (needed for `publicKey` resolution)
/// and common Mastodon/Toot extensions (`discoverable`, `featured`, etc.).
/// Activities use `WithContext::new_default` (plain AS context) — only actor
/// JSON needs the security vocab.
pub fn actor_ap_context() -> serde_json::Value {
    serde_json::json!([
        "https://www.w3.org/ns/activitystreams",
        "https://w3id.org/security/v1",
        {
            "manuallyApprovesFollowers": "as:manuallyApprovesFollowers",
            "toot": "http://joinmastodon.org/ns#",
            "discoverable": "toot:discoverable",
            "featured": {"@id": "toot:featured", "@type": "@id"}
        }
    ])
}
