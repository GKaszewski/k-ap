use std::net::IpAddr;

use url::Url;

fn is_ip_private(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            v4.is_loopback()
                || v4.is_private()
                || v4.is_link_local()
                || v4.is_broadcast()
                || v4.is_unspecified()
                || v4.octets()[0] == 100 && (v4.octets()[1] & 0xC0) == 64 // 100.64.0.0/10
        }
        IpAddr::V6(v6) => {
            v6.is_loopback()
                || v6.is_unspecified()
                || (v6.segments()[0] & 0xfe00) == 0xfc00 // fc00::/7 (ULA)
                || (v6.segments()[0] & 0xffc0) == 0xfe80 // fe80::/10 (link-local)
        }
    }
}

/// Resolve a URL's hostname and reject private/reserved IP ranges.
pub(crate) async fn validate_url(url: &Url) -> anyhow::Result<()> {
    let host = url
        .host_str()
        .ok_or_else(|| anyhow::anyhow!("URL has no host: {url}"))?;
    let port = url.port_or_known_default().unwrap_or(443);
    let addr = format!("{host}:{port}");
    let resolved = tokio::net::lookup_host(&addr).await?;
    for ip in resolved {
        if is_ip_private(ip.ip()) {
            anyhow::bail!("SSRF blocked: {url} resolves to private IP {}", ip.ip());
        }
    }
    Ok(())
}

#[derive(Clone)]
pub(crate) struct SsrfVerifier;

#[async_trait::async_trait]
impl activitypub_federation::config::UrlVerifier for SsrfVerifier {
    async fn verify(&self, url: &Url) -> Result<(), activitypub_federation::error::Error> {
        validate_url(url).await.map_err(|_| {
            activitypub_federation::error::Error::UrlVerificationError(
                "URL resolves to a private/reserved IP range",
            )
        })
    }
}

#[cfg(test)]
mod tests {
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
    fn allows_public_ipv6() {
        assert!(!is_ip_private("2001:4860:4860::8888".parse().unwrap()));
        assert!(!is_ip_private("2606:4700::1111".parse().unwrap()));
    }
}
