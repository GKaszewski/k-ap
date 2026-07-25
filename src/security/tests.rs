use super::*;

#[test]
fn rejects_ipv4_loopback() {
    assert!(is_ip_private("127.0.0.1".parse().unwrap()));
    assert!(is_ip_private("127.255.255.255".parse().unwrap()));
}

#[test]
fn rejects_ipv4_private_10() {
    assert!(is_ip_private("10.0.0.1".parse().unwrap()));
    assert!(is_ip_private("10.255.255.255".parse().unwrap()));
}

#[test]
fn rejects_ipv4_private_172() {
    assert!(is_ip_private("172.16.0.1".parse().unwrap()));
    assert!(is_ip_private("172.31.255.255".parse().unwrap()));
}

#[test]
fn rejects_ipv4_private_192() {
    assert!(is_ip_private("192.168.0.1".parse().unwrap()));
    assert!(is_ip_private("192.168.255.255".parse().unwrap()));
}

#[test]
fn rejects_ipv4_link_local() {
    assert!(is_ip_private("169.254.0.1".parse().unwrap()));
    assert!(is_ip_private("169.254.255.255".parse().unwrap()));
}

#[test]
fn rejects_ipv4_unspecified() {
    assert!(is_ip_private("0.0.0.0".parse().unwrap()));
}

#[test]
fn rejects_ipv4_cgnat() {
    assert!(is_ip_private("100.64.0.1".parse().unwrap()));
    assert!(is_ip_private("100.127.255.255".parse().unwrap()));
}

#[test]
fn rejects_ipv4_test_net() {
    assert!(is_ip_private("192.0.2.1".parse().unwrap()));
    assert!(is_ip_private("198.51.100.1".parse().unwrap()));
    assert!(is_ip_private("203.0.113.1".parse().unwrap()));
}

#[test]
fn rejects_ipv4_benchmarking() {
    assert!(is_ip_private("198.18.0.1".parse().unwrap()));
    assert!(is_ip_private("198.19.255.255".parse().unwrap()));
}

#[test]
fn rejects_ipv4_reserved() {
    assert!(is_ip_private("240.0.0.1".parse().unwrap()));
    assert!(is_ip_private("255.255.255.254".parse().unwrap()));
}

#[test]
fn allows_public_ipv4() {
    assert!(!is_ip_private("8.8.8.8".parse().unwrap()));
    assert!(!is_ip_private("1.1.1.1".parse().unwrap()));
    assert!(!is_ip_private("93.184.216.34".parse().unwrap()));
}

#[test]
fn rejects_ipv6_loopback() {
    assert!(is_ip_private("::1".parse().unwrap()));
}

#[test]
fn rejects_ipv6_unspecified() {
    assert!(is_ip_private("::".parse().unwrap()));
}

#[test]
fn rejects_ipv6_ula() {
    assert!(is_ip_private("fc00::1".parse().unwrap()));
    assert!(is_ip_private("fd12:3456::1".parse().unwrap()));
}

#[test]
fn rejects_ipv6_link_local() {
    assert!(is_ip_private("fe80::1".parse().unwrap()));
}

#[test]
fn rejects_ipv6_documentation() {
    assert!(is_ip_private("2001:db8::1".parse().unwrap()));
    assert!(is_ip_private("2001:db8:ffff::1".parse().unwrap()));
}

#[test]
fn rejects_ipv6_mapped_private_ipv4() {
    assert!(is_ip_private("::ffff:10.0.0.1".parse().unwrap()));
    assert!(is_ip_private("::ffff:127.0.0.1".parse().unwrap()));
    assert!(is_ip_private("::ffff:192.168.1.1".parse().unwrap()));
    assert!(is_ip_private("::ffff:172.16.0.1".parse().unwrap()));
}

#[test]
fn allows_ipv6_mapped_public_ipv4() {
    assert!(!is_ip_private("::ffff:8.8.8.8".parse().unwrap()));
    assert!(!is_ip_private("::ffff:1.1.1.1".parse().unwrap()));
}

#[test]
fn allows_public_ipv6() {
    assert!(!is_ip_private("2001:4860:4860::8888".parse().unwrap()));
    assert!(!is_ip_private("2606:4700::1111".parse().unwrap()));
}
