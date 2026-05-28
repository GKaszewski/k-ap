use activitypub_federation::config::{Data, FederationConfig, FederationMiddleware, UrlVerifier};
use activitypub_federation::error::Error as FedError;
use url::Url;

use crate::data::FederationData;

#[derive(Clone)]
struct PermissiveVerifier;

#[async_trait::async_trait]
impl UrlVerifier for PermissiveVerifier {
    async fn verify(&self, _url: &Url) -> Result<(), FedError> {
        Ok(())
    }
}

#[derive(Clone)]
pub struct ApFederationConfig(pub FederationConfig<FederationData>);

impl ApFederationConfig {
    /// Create a new federation config.
    ///
    /// **HTTP signature / Digest behavior:**
    /// - Production (`debug = false`): strict normalization + **requires `Digest` header** on every
    ///   inbound POST. All major AP implementations (Mastodon, Pleroma, Pixelfed) include it.
    /// - Debug (`debug = true`): relaxes Digest requirement, disables signature verification,
    ///   and accepts any URL. **Never use in production.**
    ///
    /// Outbound signing always uses Mastodon compat mode regardless of this flag.
    pub async fn new(data: FederationData, debug: bool) -> anyhow::Result<Self> {
        let config = if debug {
            FederationConfig::builder()
                .domain(&data.domain)
                .app_data(data)
                .debug(true)
                .http_signature_compat(true)
                .url_verifier(Box::new(PermissiveVerifier))
                .build()
                .await?
        } else {
            FederationConfig::builder()
                .domain(&data.domain)
                .app_data(data)
                .debug(false)
                .build()
                .await?
        };
        Ok(Self(config))
    }

    pub fn to_request_data(&self) -> Data<FederationData> {
        self.0.to_request_data()
    }

    pub fn middleware(&self) -> FederationMiddleware<FederationData> {
        FederationMiddleware::new(self.0.clone())
    }
}
