//! Request signing. Kalshi wants `timestamp_ms + METHOD + path` signed with
//! the account's private key; only Ed25519 keys (Kalshi's default) are
//! supported here.

use std::path::Path;

use anyhow::{Context, anyhow};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use chrono::Utc;
use ed25519_dalek::pkcs8::DecodePrivateKey;
use ed25519_dalek::{Signer, SigningKey};

pub struct Credentials {
    key_id: String,
    key: SigningKey,
}

impl Credentials {
    pub fn load(key_id: String, key_path: &Path) -> anyhow::Result<Self> {
        let pem = std::fs::read_to_string(key_path)
            .with_context(|| format!("reading private key {}", key_path.display()))?;
        let key = SigningKey::from_pkcs8_pem(pem.trim())
            .map_err(|e| anyhow!("{} is not an Ed25519 PKCS#8 key: {e}", key_path.display()))?;
        Ok(Self { key_id, key })
    }

    /// Auth headers for one request. Signatures embed the current time, so
    /// call this right before connecting.
    pub fn headers(&self, method: &str, path: &str) -> [(&'static str, String); 3] {
        let timestamp = Utc::now().timestamp_millis().to_string();
        let signature = self.key.sign(format!("{timestamp}{method}{path}").as_bytes());
        [
            ("KALSHI-ACCESS-KEY", self.key_id.clone()),
            ("KALSHI-ACCESS-SIGNATURE", STANDARD.encode(signature.to_bytes())),
            ("KALSHI-ACCESS-TIMESTAMP", timestamp),
        ]
    }
}
