//! Payload envelope encryption for storage adapters.

use chacha20poly1305::aead::{Aead, KeyInit};
use chacha20poly1305::{XChaCha20Poly1305, XNonce};
use rand_core::{OsRng, RngCore};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use zeroize::Zeroizing;

use crate::error::{PhotonError, Result};

const ENVELOPE_VERSION: u8 = 1;
const KEY_ENV: &str = "PHOTON_TRANSPORT_KEY";
const ALLOW_DEV_KEY_ENV: &str = "PHOTON_ALLOW_DEV_TRANSPORT_KEY";
const DEV_KEY: [u8; 32] = *b"photon-dev-transport-key-32bytes";

#[derive(Debug, Serialize, Deserialize)]
struct TransportEnvelope {
    version: u8,
    actor_json: Value,
    payload_json: Value,
}

/// Symmetric payload encryption (XChaCha20-Poly1305).
#[derive(Clone)]
pub struct TransportCrypto {
    key: Zeroizing<[u8; 32]>,
}

impl TransportCrypto {
    /// Build from an explicit 32-byte key.
    ///
    /// Callers own key material and secrecy. Prefer this in tests and when wiring a
    /// key from a secrets manager rather than the process environment.
    #[must_use]
    pub fn from_bytes(key: [u8; 32]) -> Self {
        Self {
            key: Zeroizing::new(key),
        }
    }

    /// Load key from `PHOTON_TRANSPORT_KEY` (standard base64 encoding of exactly 32 bytes).
    ///
    /// # Errors
    ///
    /// Returns [`PhotonError::Internal`] when the variable is missing, not valid
    /// base64, or does not decode to 32 bytes. Production hosts should fail closed here.
    ///
    /// # Contract
    ///
    /// Does not fall back to a development key. Use [`Self::from_env_or_dev_default`]
    /// only with an explicit development opt-in.
    pub fn from_env() -> Result<Self> {
        let raw = std::env::var(KEY_ENV).map_err(|_| {
            PhotonError::Internal(format!(
                "{KEY_ENV} is required (base64-encoded 32-byte transport key). \
                 For local development only, set {ALLOW_DEV_KEY_ENV}=1 to allow the \
                 hard-coded development key via from_env_or_dev_default()"
            ))
        })?;
        Self::from_base64(raw.trim())
    }

    /// Load key from `PHOTON_TRANSPORT_KEY`, or the hard-coded development key when
    /// explicitly opted in.
    ///
    /// **Development-only.** The hard-coded key is used only when
    /// `PHOTON_ALLOW_DEV_TRANSPORT_KEY` is `1` or `true` **and** `PHOTON_TRANSPORT_KEY`
    /// is unset or invalid. A loud warning is printed when the development key is used.
    ///
    /// Prefer [`Self::from_env`] in production and CI (set a real `PHOTON_TRANSPORT_KEY`).
    ///
    /// # Errors
    ///
    /// Returns an error when the environment key is missing/invalid and the
    /// development-key opt-in is not set.
    pub fn from_env_or_dev_default() -> Result<Self> {
        match Self::from_env() {
            Ok(crypto) => Ok(crypto),
            Err(env_err) => {
                if allow_dev_transport_key() {
                    tracing::warn!(
                        env = ALLOW_DEV_KEY_ENV,
                        key_env = KEY_ENV,
                        "Photon is using the hard-coded development transport key; do not use in production"
                    );
                    Ok(Self::from_bytes(DEV_KEY))
                } else {
                    Err(env_err)
                }
            }
        }
    }

    fn from_base64(s: &str) -> Result<Self> {
        let bytes =
            base64::Engine::decode(&base64::engine::general_purpose::STANDARD, s).map_err(|e| {
                PhotonError::caused(format!("{KEY_ENV} is not valid standard base64"), e)
            })?;
        if bytes.len() != 32 {
            return Err(PhotonError::Internal(format!(
                "{KEY_ENV} must decode to exactly 32 bytes (got {})",
                bytes.len()
            )));
        }
        let mut key = [0u8; 32];
        key.copy_from_slice(&bytes);
        Ok(Self::from_bytes(key))
    }

    /// Encrypt actor + payload JSON into opaque ciphertext bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if the operation fails.
    pub fn encrypt(&self, actor_json: &Value, payload_json: &Value) -> Result<Vec<u8>> {
        let plaintext = serde_json::to_vec(&TransportEnvelope {
            version: ENVELOPE_VERSION,
            actor_json: actor_json.clone(),
            payload_json: payload_json.clone(),
        })?;
        if bench_crypto_disabled() {
            return Ok(plaintext);
        }
        self.seal(&plaintext)
    }

    /// Decrypt ciphertext into actor + payload JSON.
    ///
    /// # Errors
    ///
    /// Returns an error if the operation fails.
    pub fn decrypt(&self, ciphertext: &[u8]) -> Result<(Value, Value)> {
        let plaintext = if bench_crypto_disabled() {
            ciphertext.to_vec()
        } else {
            self.open(ciphertext)?
        };
        let env: TransportEnvelope = serde_json::from_slice(&plaintext)
            .map_err(|e| PhotonError::PayloadError(e.to_string()))?;
        if env.version != ENVELOPE_VERSION {
            return Err(PhotonError::PayloadError(format!(
                "unsupported transport envelope version {}",
                env.version
            )));
        }
        Ok((env.actor_json, env.payload_json))
    }

    fn seal(&self, plaintext: &[u8]) -> Result<Vec<u8>> {
        let cipher = XChaCha20Poly1305::new_from_slice(self.key.as_slice())
            .map_err(|e| PhotonError::caused("transport seal key", e))?;
        let mut nonce = [0u8; 24];
        OsRng.fill_bytes(&mut nonce);
        let ct = cipher
            .encrypt(XNonce::from_slice(&nonce), plaintext)
            .map_err(|e| PhotonError::caused("transport seal encrypt", e))?;
        let mut out = Vec::with_capacity(24 + ct.len());
        out.extend_from_slice(&nonce);
        out.extend_from_slice(&ct);
        Ok(out)
    }

    fn open(&self, ciphertext: &[u8]) -> Result<Vec<u8>> {
        if ciphertext.len() < 24 {
            return Err(PhotonError::PayloadError(
                "transport ciphertext too short".into(),
            ));
        }
        let (nonce, ct) = ciphertext.split_at(24);
        let cipher = XChaCha20Poly1305::new_from_slice(self.key.as_slice())
            .map_err(|e| PhotonError::caused("transport open key", e))?;
        cipher
            .decrypt(XNonce::from_slice(nonce), ct)
            .map_err(|e| PhotonError::caused("transport open decrypt", e))
    }
}

fn allow_dev_transport_key() -> bool {
    matches!(
        std::env::var(ALLOW_DEV_KEY_ENV).as_deref(),
        Ok("1" | "true" | "TRUE" | "yes" | "YES")
    )
}

fn bench_crypto_disabled() -> bool {
    matches!(
        std::env::var("PHOTON_BENCH_CRYPTO").as_deref(),
        Ok("0" | "false" | "FALSE" | "no" | "NO")
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn roundtrip_encrypt_decrypt() {
        let crypto = TransportCrypto::from_bytes(DEV_KEY);
        let actor = json!({"System": {"operation": "test"}});
        let payload = json!({"n": 1});
        let ct = crypto.encrypt(&actor, &payload).expect("encrypt");
        let (a, p) = crypto.decrypt(&ct).expect("decrypt");
        assert_eq!(a, actor);
        assert_eq!(p, payload);
    }
}
