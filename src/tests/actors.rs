use super::*;

// ── Person AP JSON serialization ──────────────────────────────────────────────

#[test]
fn person_serializes_with_enriched_fields() {
    let person = Person {
        kind: Default::default(),
        id: "https://example.com/users/1"
            .parse::<url::Url>()
            .unwrap()
            .into(),
        preferred_username: "alice".to_string(),
        inbox: "https://example.com/users/1/inbox".parse().unwrap(),
        outbox: Some("https://example.com/users/1/outbox".parse().unwrap()),
        followers: Some("https://example.com/users/1/followers".parse().unwrap()),
        following: Some("https://example.com/users/1/following".parse().unwrap()),
        public_key: activitypub_federation::protocol::public_key::PublicKey {
            id: "https://example.com/users/1#main-key".to_string(),
            owner: "https://example.com/users/1".parse().unwrap(),
            public_key_pem: "pem".to_string(),
        },
        name: Some("Alice".to_string()),
        summary: Some("Bio text".to_string()),
        icon: Some(ApImageObject {
            kind: "Image".to_string(),
            url: "https://example.com/images/avatars/1".parse().unwrap(),
        }),
        url: Some("https://example.com/u/alice".parse().unwrap()),
        discoverable: Some(true),
        manually_approves_followers: true,
        updated: Some(chrono::Utc::now()),
        endpoints: Some(Endpoints {
            shared_inbox: "https://example.com/inbox".parse().unwrap(),
        }),
        image: None,
        also_known_as: vec![],
        attachment: vec![],
        featured: Some("https://example.com/users/1/featured".parse().unwrap()),
    };
    let json = serde_json::to_value(&person).unwrap();
    assert_eq!(json["discoverable"], true);
    assert_eq!(json["summary"], "Bio text");
    assert_eq!(json["icon"]["type"], "Image");
    assert_eq!(json["manuallyApprovesFollowers"], true);
    assert!(json.get("updated").is_some());
    assert!(json.get("endpoints").is_some());
    assert_eq!(
        json["endpoints"]["sharedInbox"],
        "https://example.com/inbox"
    );
    assert_eq!(json["featured"], "https://example.com/users/1/featured");
}

#[test]
fn person_actor_type_service_serializes_correctly() {
    let mut person = minimal_person();
    person.kind = crate::user::ApActorType::Service;
    let json = serde_json::to_value(&person).unwrap();
    assert_eq!(json["type"], "Service");
}

#[test]
fn person_discoverable_false_serializes() {
    let mut person = minimal_person();
    person.discoverable = Some(false);
    let json = serde_json::to_value(&person).unwrap();
    assert_eq!(json["discoverable"], false);
}

#[test]
fn person_also_known_as_serializes_as_array() {
    let mut person = minimal_person();
    person.also_known_as = vec![
        "https://old.example/users/alice".to_string(),
        "https://other.example/users/alice".to_string(),
    ];
    let json = serde_json::to_value(&person).unwrap();
    assert!(
        json["alsoKnownAs"].is_array(),
        "alsoKnownAs must serialize as a JSON array"
    );
    assert_eq!(json["alsoKnownAs"].as_array().unwrap().len(), 2);
}

#[test]
fn person_omits_optional_fields_when_none() {
    let person = minimal_person();
    let json = serde_json::to_value(&person).unwrap();
    assert!(
        json.get("summary").is_none(),
        "null summary should be omitted"
    );
    assert!(json.get("icon").is_none(), "null icon should be omitted");
    assert!(
        json.get("featured").is_none(),
        "null featured should be omitted"
    );
    assert!(json.get("url").is_none(), "null url should be omitted");
}

#[test]
fn person_featured_omitted_when_none() {
    let mut person = minimal_person();
    person.featured = None;
    let json = serde_json::to_value(&person).unwrap();
    assert!(json.get("featured").is_none());
}

// ── helper ────────────────────────────────────────────────────────────────────

fn minimal_person() -> Person {
    Person {
        kind: Default::default(),
        id: "https://example.com/users/1"
            .parse::<url::Url>()
            .unwrap()
            .into(),
        preferred_username: "alice".to_string(),
        inbox: "https://example.com/users/1/inbox".parse().unwrap(),
        outbox: None,
        followers: None,
        following: None,
        public_key: activitypub_federation::protocol::public_key::PublicKey {
            id: "https://example.com/users/1#main-key".to_string(),
            owner: "https://example.com/users/1".parse().unwrap(),
            public_key_pem: "pem".to_string(),
        },
        name: None,
        summary: None,
        icon: None,
        url: None,
        discoverable: None,
        manually_approves_followers: false,
        updated: None,
        endpoints: None,
        image: None,
        also_known_as: vec![],
        attachment: vec![],
        featured: None,
    }
}
