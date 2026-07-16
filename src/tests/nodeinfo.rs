use super::*;

#[test]
fn nodeinfo_well_known_serializes_correctly() {
    let doc = NodeInfoWellKnown {
        links: vec![NodeInfoLink {
            rel: "http://nodeinfo.diaspora.software/ns/schema/2.0".to_string(),
            href: "https://example.com/nodeinfo/2.0".to_string(),
        }],
    };
    let json = serde_json::to_value(&doc).unwrap();
    assert_eq!(
        json["links"][0]["rel"],
        "http://nodeinfo.diaspora.software/ns/schema/2.0"
    );
    assert_eq!(json["links"][0]["href"], "https://example.com/nodeinfo/2.0");
}

#[test]
fn nodeinfo_serializes_camel_case() {
    let doc = NodeInfo {
        version: "2.0".to_string(),
        software: NodeInfoSoftware {
            name: "my-app".to_string(),
            version: "0.1.0".to_string(),
        },
        protocols: vec!["activitypub".to_string()],
        services: NodeInfoServices {
            inbound: vec![],
            outbound: vec![],
        },
        open_registrations: false,
        usage: NodeInfoUsage {
            users: NodeInfoUsers { total: 3 },
            local_posts: 42,
        },
        metadata: serde_json::json!({}),
    };
    let json = serde_json::to_value(&doc).unwrap();
    assert!(json.get("$schema").is_none());
    assert_eq!(json["version"], "2.0");
    assert_eq!(json["software"]["name"], "my-app");
    assert_eq!(json["usage"]["users"]["total"], 3);
    assert_eq!(json["usage"]["localPosts"], 42);
    assert_eq!(json["openRegistrations"], false);
    assert_eq!(json["services"]["inbound"], serde_json::json!([]));
    assert_eq!(json["services"]["outbound"], serde_json::json!([]));
    assert_eq!(json["metadata"], serde_json::json!({}));
}
