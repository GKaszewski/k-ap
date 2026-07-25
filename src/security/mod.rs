use std::net::{IpAddr, Ipv4Addr};

use url::Url;

fn is_ipv4_private(v4: Ipv4Addr) -> bool {
    v4.is_loopback()
        || v4.is_private()
        || v4.is_link_local()
        || v4.is_broadcast()
        || v4.is_unspecified()
        || (v4.octets()[0] == 100 && (v4.octets()[1] & 0xC0) == 64) // CGNAT 100.64.0.0/10
        || v4.octets()[0] == 0 // 0.0.0.0/8 "this network"
        || (v4.octets()[0] == 192 && v4.octets()[1] == 0 && v4.octets()[2] == 2) // TEST-NET-1
        || (v4.octets()[0] == 198 && v4.octets()[1] == 51 && v4.octets()[2] == 100) // TEST-NET-2
        || (v4.octets()[0] == 203 && v4.octets()[1] == 0 && v4.octets()[2] == 113) // TEST-NET-3
        || (v4.octets()[0] == 198 && (v4.octets()[1] & 0xFE) == 18) // benchmarking 198.18.0.0/15
        || v4.octets()[0] >= 240 // reserved 240.0.0.0/4
}

fn is_ip_private(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => is_ipv4_private(v4),
        IpAddr::V6(v6) => {
            if let Some(mapped_v4) = v6.to_ipv4_mapped() {
                return is_ipv4_private(mapped_v4);
            }

            v6.is_loopback()
                || v6.is_unspecified()
                || (v6.segments()[0] & 0xfe00) == 0xfc00 // ULA fc00::/7
                || (v6.segments()[0] & 0xffc0) == 0xfe80 // link-local fe80::/10
                || (v6.segments()[0] == 0x2001 && v6.segments()[1] == 0x0db8) // documentation 2001:db8::/32
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
#[path = "tests.rs"]
mod tests;
