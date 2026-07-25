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
    let addressing = visibility_addressing(ApVisibility::Public, &followers_url());
    assert_eq!(addressing.to, vec![AS_PUBLIC.to_string()]);
    assert_eq!(addressing.cc, vec![followers_url().to_string()]);
}

#[test]
fn followers_only_visibility_addresses_followers_only() {
    let addressing = visibility_addressing(ApVisibility::FollowersOnly, &followers_url());
    assert_eq!(addressing.to, vec![followers_url().to_string()]);
    assert!(
        addressing.cc.is_empty(),
        "FollowersOnly must not include AS_PUBLIC in cc"
    );
}

#[test]
fn followers_only_excludes_as_public() {
    let addressing = visibility_addressing(ApVisibility::FollowersOnly, &followers_url());
    assert!(
        !addressing.to.contains(&AS_PUBLIC.to_string()),
        "FollowersOnly must not include AS_PUBLIC in to"
    );
    assert!(
        !addressing.cc.contains(&AS_PUBLIC.to_string()),
        "FollowersOnly must not include AS_PUBLIC in cc"
    );
}

#[test]
fn private_visibility_produces_empty_addressing() {
    let addressing = visibility_addressing(ApVisibility::Private, &followers_url());
    assert!(addressing.to.is_empty());
    assert!(addressing.cc.is_empty());
}

#[test]
fn public_and_followers_only_differ_in_to() {
    let public = visibility_addressing(ApVisibility::Public, &followers_url());
    let followers_only = visibility_addressing(ApVisibility::FollowersOnly, &followers_url());
    assert_ne!(
        public.to, followers_only.to,
        "Public and FollowersOnly must produce different to fields"
    );
}
