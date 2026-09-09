//! Owner-local catalog paging and authenticated opaque cursor support.

use super::{
    session_management::{rpc_error, SessionManagementFailure},
    DaemonServer,
};
use crate::transport::AnyTransportWriter;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use hmac::{Hmac, Mac};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sha2::Sha256;
use std::sync::Arc;
use whale_protocol::{
    rpc::{JSONRPCError, JSONRPCResponse, RequestId},
    session_management::{
        ListSessionsParams, MAX_SESSION_CURSOR_TOKEN_BYTES, SESSION_MANAGEMENT_CURSOR_TTL_MS,
    },
};

pub(super) const LIST_CURSOR_DOMAIN: &[u8] = b"whale.session-list.v1";
pub(super) const HISTORY_CURSOR_DOMAIN: &[u8] = b"whale.session-history.v1";
const CURSOR_VERSION: u8 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CursorDecodeError {
    Invalid,
}

#[derive(Clone)]
pub(super) struct CursorCodec {
    key: Arc<[u8; 32]>,
    origin: tokio::time::Instant,
}

impl Default for CursorCodec {
    fn default() -> Self {
        let mut key = [0_u8; 32];
        getrandom::fill(&mut key).expect("operating-system cursor key generation failed");
        Self::with_key(key)
    }
}

impl CursorCodec {
    fn with_key(key: [u8; 32]) -> Self {
        Self {
            key: Arc::new(key),
            origin: tokio::time::Instant::now(),
        }
    }

    #[cfg(test)]
    pub(super) fn fixed_for_test(key: [u8; 32]) -> Self {
        Self::with_key(key)
    }

    pub(super) fn expiry(&self) -> Result<u64, SessionManagementFailure> {
        self.now_ms()
            .checked_add(SESSION_MANAGEMENT_CURSOR_TTL_MS)
            .ok_or_else(|| {
                SessionManagementFailure::InvalidProjection(
                    "Session cursor expiry is exhausted".into(),
                )
            })
    }

    pub(super) fn is_expired(&self, expiry_ms: u64) -> bool {
        self.now_ms() >= expiry_ms
    }

    fn now_ms(&self) -> u64 {
        u64::try_from(
            tokio::time::Instant::now()
                .saturating_duration_since(self.origin)
                .as_millis(),
        )
        .unwrap_or(u64::MAX)
    }

    pub(super) fn encode<T: Serialize>(
        &self,
        domain: &[u8],
        owner: &str,
        payload: &T,
    ) -> Result<String, SessionManagementFailure> {
        let payload = serde_json::to_vec(payload).map_err(|error| {
            SessionManagementFailure::InvalidProjection(format!(
                "Session cursor payload did not serialize: {error}"
            ))
        })?;
        let mut mac = Hmac::<Sha256>::new_from_slice(self.key.as_ref())
            .expect("HMAC-SHA256 accepts a 32-byte key");
        update_mac(&mut mac, domain, owner, &payload);
        let tag = mac.finalize().into_bytes();
        let token = format!(
            "{}.{}",
            URL_SAFE_NO_PAD.encode(payload),
            URL_SAFE_NO_PAD.encode(tag)
        );
        if token.len() > MAX_SESSION_CURSOR_TOKEN_BYTES {
            return Err(SessionManagementFailure::InvalidProjection(
                "Session cursor token exceeds the wire limit".into(),
            ));
        }
        Ok(token)
    }

    pub(super) fn decode<T: DeserializeOwned>(
        &self,
        domain: &[u8],
        owner: &str,
        token: &str,
    ) -> Result<T, CursorDecodeError> {
        if token.len() > MAX_SESSION_CURSOR_TOKEN_BYTES {
            return Err(CursorDecodeError::Invalid);
        }
        let mut parts = token.split('.');
        let payload = parts.next().ok_or(CursorDecodeError::Invalid)?;
        let tag = parts.next().ok_or(CursorDecodeError::Invalid)?;
        if parts.next().is_some() || payload.is_empty() || tag.is_empty() {
            return Err(CursorDecodeError::Invalid);
        }
        let payload = URL_SAFE_NO_PAD
            .decode(payload)
            .map_err(|_| CursorDecodeError::Invalid)?;
        let tag = URL_SAFE_NO_PAD
            .decode(tag)
            .map_err(|_| CursorDecodeError::Invalid)?;
        let mut mac = Hmac::<Sha256>::new_from_slice(self.key.as_ref())
            .expect("HMAC-SHA256 accepts a 32-byte key");
        update_mac(&mut mac, domain, owner, &payload);
        mac.verify_slice(&tag)
            .map_err(|_| CursorDecodeError::Invalid)?;
        serde_json::from_slice(&payload).map_err(|_| CursorDecodeError::Invalid)
    }
}

fn update_mac(mac: &mut Hmac<Sha256>, domain: &[u8], owner: &str, payload: &[u8]) {
    mac.update(domain);
    mac.update(&[0]);
    mac.update(owner.as_bytes());
    mac.update(&[0]);
    mac.update(payload);
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ListCursorClaims {
    version: u8,
    kind: String,
    pub(super) generation: u64,
    pub(super) after_ordinal: u64,
    pub(super) through_ordinal: u64,
    pub(super) expires_at_ms: u64,
}

impl ListCursorClaims {
    pub(super) fn new(
        generation: u64,
        after_ordinal: u64,
        through_ordinal: u64,
        expires_at_ms: u64,
    ) -> Self {
        Self {
            version: CURSOR_VERSION,
            kind: "session_list".into(),
            generation,
            after_ordinal,
            through_ordinal,
            expires_at_ms,
        }
    }

    pub(super) fn valid_shape(&self) -> bool {
        self.version == CURSOR_VERSION
            && self.kind == "session_list"
            && self.after_ordinal <= self.through_ordinal
    }
}

impl DaemonServer {
    pub(super) fn handle_session_catalog(
        &self,
        id: RequestId,
        params: Option<serde_json::Value>,
        transport: &AnyTransportWriter,
    ) -> JSONRPCResponse {
        let params: ListSessionsParams =
            match serde_json::from_value(params.unwrap_or(serde_json::Value::Null)) {
                Ok(params) => params,
                Err(error) => {
                    return JSONRPCResponse::error(
                        id,
                        JSONRPCError::invalid_params(error.to_string()),
                    )
                }
            };
        match self
            .session_management
            .list_sessions(transport.connection_id(), &params)
        {
            Ok(result) => {
                JSONRPCResponse::success(id, result).expect("Session catalog result serializes")
            }
            Err(error) => JSONRPCResponse::error(id, rpc_error(error)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn cursor_authentication_binds_kind_owner_key_payload_and_expiry() {
        let codec = CursorCodec::fixed_for_test([7; 32]);
        let claims = ListCursorClaims::new(3, 4, 9, codec.expiry().unwrap());
        let token = codec
            .encode(LIST_CURSOR_DOMAIN, "owner-a", &claims)
            .unwrap();
        let decoded: ListCursorClaims =
            codec.decode(LIST_CURSOR_DOMAIN, "owner-a", &token).unwrap();
        assert_eq!(decoded, claims);
        let encoded_payload = token.split('.').next().unwrap();
        let wire_payload = URL_SAFE_NO_PAD.decode(encoded_payload).unwrap();
        assert!(!String::from_utf8(wire_payload).unwrap().contains("owner-a"));

        assert_eq!(
            codec.decode::<ListCursorClaims>(LIST_CURSOR_DOMAIN, "owner-b", &token),
            Err(CursorDecodeError::Invalid)
        );
        assert_eq!(
            codec.decode::<ListCursorClaims>(HISTORY_CURSOR_DOMAIN, "owner-a", &token),
            Err(CursorDecodeError::Invalid)
        );
        assert_eq!(
            CursorCodec::fixed_for_test([8; 32]).decode::<ListCursorClaims>(
                LIST_CURSOR_DOMAIN,
                "owner-a",
                &token
            ),
            Err(CursorDecodeError::Invalid)
        );

        let mut forged = token.into_bytes();
        forged[0] = if forged[0] == b'A' { b'B' } else { b'A' };
        let forged = String::from_utf8(forged).unwrap();
        assert_eq!(
            codec.decode::<ListCursorClaims>(LIST_CURSOR_DOMAIN, "owner-a", &forged),
            Err(CursorDecodeError::Invalid)
        );
        let token = codec
            .encode(LIST_CURSOR_DOMAIN, "owner-a", &claims)
            .unwrap();
        let mut forged_tag = token.into_bytes();
        let tag_start = forged_tag.iter().position(|byte| *byte == b'.').unwrap() + 1;
        forged_tag[tag_start] = if forged_tag[tag_start] == b'A' {
            b'B'
        } else {
            b'A'
        };
        let forged_tag = String::from_utf8(forged_tag).unwrap();
        assert_eq!(
            codec.decode::<ListCursorClaims>(LIST_CURSOR_DOMAIN, "owner-a", &forged_tag),
            Err(CursorDecodeError::Invalid)
        );
        assert_eq!(
            codec.decode::<ListCursorClaims>(LIST_CURSOR_DOMAIN, "owner-a", "%%%"),
            Err(CursorDecodeError::Invalid)
        );
        assert_eq!(
            codec.decode::<ListCursorClaims>(
                LIST_CURSOR_DOMAIN,
                "owner-a",
                &"x".repeat(MAX_SESSION_CURSOR_TOKEN_BYTES + 1),
            ),
            Err(CursorDecodeError::Invalid)
        );

        tokio::time::advance(std::time::Duration::from_millis(
            SESSION_MANAGEMENT_CURSOR_TTL_MS,
        ))
        .await;
        assert!(codec.is_expired(claims.expires_at_ms));
    }
}
