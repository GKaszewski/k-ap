/// Tests for broadcast addressing logic (visibility → to/cc fields).
use url::Url;

use crate::service::broadcast::visibility_addressing;
use crate::urls::AS_PUBLIC;
use crate::user::ApVisibility;

fn followers_url() -> Url {
    "https://example.com/users/alice/followers".parse().unwrap()
}

#[test]
fn public_visibility_addresses_public_and_followers() {
    let (to, cc) = visibility_addressing(ApVisibility::Public, &followers_url());
    assert_eq!(to, vec![AS_PUBLIC.to_string()]);
    assert_eq!(cc, vec![followers_url().to_string()]);
}

#[test]
fn followers_only_visibility_addresses_followers_only() {
    let (to, cc) = visibility_addressing(ApVisibility::FollowersOnly, &followers_url());
    assert_eq!(to, vec![followers_url().to_string()]);
    assert!(
        cc.is_empty(),
        "FollowersOnly must not include AS_PUBLIC in cc"
    );
}

#[test]
fn followers_only_excludes_as_public() {
    let (to, cc) = visibility_addressing(ApVisibility::FollowersOnly, &followers_url());
    assert!(
        !to.contains(&AS_PUBLIC.to_string()),
        "FollowersOnly must not include AS_PUBLIC in to"
    );
    assert!(
        !cc.contains(&AS_PUBLIC.to_string()),
        "FollowersOnly must not include AS_PUBLIC in cc"
    );
}

#[test]
fn private_visibility_produces_empty_addressing() {
    let (to, cc) = visibility_addressing(ApVisibility::Private, &followers_url());
    assert!(to.is_empty());
    assert!(cc.is_empty());
}

#[test]
fn public_and_followers_only_differ_in_to() {
    let (pub_to, _) = visibility_addressing(ApVisibility::Public, &followers_url());
    let (fo_to, _) = visibility_addressing(ApVisibility::FollowersOnly, &followers_url());
    assert_ne!(
        pub_to, fo_to,
        "Public and FollowersOnly must produce different to fields"
    );
}
