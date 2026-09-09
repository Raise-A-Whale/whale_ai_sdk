//! Bounded canonical-history archive and backward fixed-window paging.

use super::{
    session_catalog::{CursorCodec, HISTORY_CURSOR_DOMAIN},
    session_management::{rpc_error, SessionManagementFailure},
    DaemonServer,
};
use crate::transport::AnyTransportWriter;
use serde::{Deserialize, Serialize};
use std::collections::{HashSet, VecDeque};
use whale_protocol::{
    canonical::CanonicalItem,
    retention::serialized_bytes,
    rpc::{JSONRPCError, JSONRPCResponse, RequestId},
    session_management::{
        GetSessionHistoryParams, GetSessionHistoryResult, SessionHistoryAnchor,
        SessionHistoryPageCursor, MAX_SESSION_MANAGEMENT_HISTORY_BYTES,
        MAX_SESSION_MANAGEMENT_PAGE_BYTES,
    },
};

const HISTORY_CURSOR_VERSION: u8 = 1;
const MAX_SESSION_MANAGEMENT_HISTORY_ITEMS: usize = 16 * 1024;

#[derive(Clone)]
struct ArchivedItem {
    item: CanonicalItem,
    bytes: usize,
}

#[derive(Clone)]
pub(super) struct HistoryArchive {
    thread_id: String,
    stream_id: String,
    floor: u64,
    end: u64,
    items: VecDeque<ArchivedItem>,
    bytes: usize,
    max_items: usize,
    max_bytes: usize,
}

impl HistoryArchive {
    pub(super) fn new(
        thread_id: String,
        stream_id: String,
        history: Vec<CanonicalItem>,
        configured_max_bytes: Option<u64>,
    ) -> Result<Self, SessionManagementFailure> {
        let configured_max_bytes = configured_max_bytes
            .and_then(|limit| usize::try_from(limit).ok())
            .unwrap_or(usize::MAX);
        let mut archive = Self {
            thread_id,
            stream_id,
            floor: 0,
            end: 0,
            items: VecDeque::new(),
            bytes: 0,
            max_items: MAX_SESSION_MANAGEMENT_HISTORY_ITEMS,
            max_bytes: MAX_SESSION_MANAGEMENT_HISTORY_BYTES.min(configured_max_bytes),
        };
        archive.append(&history)?;
        Ok(archive)
    }

    #[cfg(test)]
    fn with_limits(thread_id: &str, stream_id: &str, max_items: usize, max_bytes: usize) -> Self {
        Self {
            thread_id: thread_id.into(),
            stream_id: stream_id.into(),
            floor: 0,
            end: 0,
            items: VecDeque::new(),
            bytes: 0,
            max_items,
            max_bytes,
        }
    }

    pub(super) fn prepare_append(
        &self,
        items: &[CanonicalItem],
    ) -> Result<Self, SessionManagementFailure> {
        let mut next = self.clone();
        next.append(items)?;
        Ok(next)
    }

    fn append(&mut self, items: &[CanonicalItem]) -> Result<(), SessionManagementFailure> {
        let mut identities: HashSet<String> = self
            .items
            .iter()
            .map(|item| item.item.id().to_owned())
            .collect();
        for item in items {
            if item.id().trim().is_empty() || !identities.insert(item.id().to_owned()) {
                return Err(SessionManagementFailure::InvalidProjection(
                    "Canonical history item identities must be nonempty and unique".into(),
                ));
            }
            let bytes =
                usize::try_from(serialized_bytes(item).map_err(|error| {
                    SessionManagementFailure::InvalidProjection(error.to_string())
                })?)
                .map_err(|_| {
                    SessionManagementFailure::InvalidProjection(
                        "Canonical history item byte size exceeds usize".into(),
                    )
                })?;
            self.end = self.end.checked_add(1).ok_or_else(|| {
                SessionManagementFailure::InvalidProjection(
                    "Canonical history index is exhausted".into(),
                )
            })?;
            self.bytes = self.bytes.checked_add(bytes).ok_or_else(|| {
                SessionManagementFailure::InvalidProjection(
                    "Canonical history byte count is exhausted".into(),
                )
            })?;
            self.items.push_back(ArchivedItem {
                item: item.clone(),
                bytes,
            });
            while self.items.len() > self.max_items || self.bytes > self.max_bytes {
                let evicted = self
                    .items
                    .pop_front()
                    .expect("nonempty history exceeds a retention bound");
                identities.remove(evicted.item.id());
                self.bytes -= evicted.bytes;
                self.floor = self.floor.checked_add(1).ok_or_else(|| {
                    SessionManagementFailure::InvalidProjection(
                        "Canonical history floor is exhausted".into(),
                    )
                })?;
            }
        }
        debug_assert_eq!(self.end - self.floor, self.items.len() as u64);
        Ok(())
    }

    pub(super) fn end(&self) -> u64 {
        self.end
    }

    pub(super) fn retained_bytes(&self) -> usize {
        self.bytes
    }

    pub(super) fn page(
        &self,
        codec: &CursorCodec,
        owner: &str,
        params: &GetSessionHistoryParams,
    ) -> Result<GetSessionHistoryResult, SessionManagementFailure> {
        params
            .validate()
            .map_err(SessionManagementFailure::InvalidParams)?;
        let current = self.anchor(self.end);
        let (through, before) = if let Some(cursor) = &params.cursor {
            let claims: HistoryCursorClaims = codec
                .decode(HISTORY_CURSOR_DOMAIN, owner, cursor.as_str())
                .map_err(|_| SessionManagementFailure::HistoryCursorInvalid)?;
            if !claims.valid_shape() || codec.is_expired(claims.expires_at_ms) {
                return Err(SessionManagementFailure::HistoryCursorInvalid);
            }
            if claims.thread_id != self.thread_id {
                return Err(SessionManagementFailure::HistoryCursorInvalid);
            }
            if claims.stream_id != self.stream_id {
                return Err(SessionManagementFailure::HistoryStreamReset {
                    requested: SessionHistoryAnchor {
                        thread_id: claims.thread_id,
                        stream_id: claims.stream_id,
                        index: claims.before,
                    },
                    current,
                });
            }
            if claims.through > self.end {
                return Err(SessionManagementFailure::HistoryCursorInvalid);
            }
            (claims.through, claims.before)
        } else if let Some(before) = &params.before {
            if before.thread_id != self.thread_id || before.stream_id != self.stream_id {
                return Err(SessionManagementFailure::HistoryStreamReset {
                    requested: before.clone(),
                    current,
                });
            }
            let through = before.index.min(self.end);
            (through, through)
        } else {
            (self.end, self.end)
        };

        if before < self.floor {
            return Err(SessionManagementFailure::HistoryGap {
                requested: self.anchor(before),
                floor: self.anchor(self.floor),
                current: self.anchor(self.end),
            });
        }
        if before > through || through > self.end {
            return Err(SessionManagementFailure::HistoryCursorInvalid);
        }

        let mut start = before;
        let mut items = VecDeque::new();
        let mut accepted: Option<GetSessionHistoryResult> = None;
        while start > self.floor && items.len() < params.limit as usize {
            let candidate_index = start - 1;
            let offset = usize::try_from(candidate_index - self.floor).map_err(|_| {
                SessionManagementFailure::InvalidProjection(
                    "Canonical history offset exceeds usize".into(),
                )
            })?;
            let candidate = self.items.get(offset).ok_or_else(|| {
                SessionManagementFailure::InvalidProjection(
                    "Canonical history archive indexes are inconsistent".into(),
                )
            })?;
            items.push_front(candidate.item.clone());
            let candidate_start = candidate_index;
            let result = self.result(codec, owner, candidate_start, before, through, &items)?;
            let actual = serialized_bytes(&result)
                .map_err(|error| SessionManagementFailure::InvalidProjection(error.to_string()))?;
            if actual > MAX_SESSION_MANAGEMENT_PAGE_BYTES as u64 {
                items.pop_front();
                if items.is_empty() {
                    return Err(SessionManagementFailure::ResourceLimit {
                        resource: "session_history_page".into(),
                        actual,
                        limit: MAX_SESSION_MANAGEMENT_PAGE_BYTES as u64,
                        item_index: Some(candidate_index),
                    });
                }
                break;
            }
            start = candidate_start;
            accepted = Some(result);
        }

        let result = match accepted {
            Some(result) => result,
            None => self.result(codec, owner, before, before, through, &items)?,
        };
        result
            .validate_for(params)
            .map_err(SessionManagementFailure::InvalidProjection)?;
        Ok(result)
    }

    fn result(
        &self,
        codec: &CursorCodec,
        owner: &str,
        start: u64,
        end: u64,
        through: u64,
        items: &VecDeque<CanonicalItem>,
    ) -> Result<GetSessionHistoryResult, SessionManagementFailure> {
        let next_cursor = if start > self.floor {
            let claims = HistoryCursorClaims::new(
                self.thread_id.clone(),
                self.stream_id.clone(),
                through,
                start,
                codec.expiry()?,
            );
            let token = codec.encode(HISTORY_CURSOR_DOMAIN, owner, &claims)?;
            Some(
                SessionHistoryPageCursor::new(token)
                    .map_err(SessionManagementFailure::InvalidProjection)?,
            )
        } else {
            None
        };
        Ok(GetSessionHistoryResult {
            items: items.iter().cloned().collect(),
            start_index: start,
            end_index: end,
            through: self.anchor(through),
            current_end: self.anchor(self.end),
            next_cursor,
        })
    }

    fn anchor(&self, index: u64) -> SessionHistoryAnchor {
        SessionHistoryAnchor {
            thread_id: self.thread_id.clone(),
            stream_id: self.stream_id.clone(),
            index,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct HistoryCursorClaims {
    version: u8,
    kind: String,
    thread_id: String,
    stream_id: String,
    through: u64,
    before: u64,
    expires_at_ms: u64,
}

impl HistoryCursorClaims {
    fn new(
        thread_id: String,
        stream_id: String,
        through: u64,
        before: u64,
        expires_at_ms: u64,
    ) -> Self {
        Self {
            version: HISTORY_CURSOR_VERSION,
            kind: "session_history".into(),
            thread_id,
            stream_id,
            through,
            before,
            expires_at_ms,
        }
    }

    fn valid_shape(&self) -> bool {
        self.version == HISTORY_CURSOR_VERSION
            && self.kind == "session_history"
            && self.before <= self.through
            && !self.thread_id.trim().is_empty()
            && !self.stream_id.trim().is_empty()
    }
}

impl DaemonServer {
    pub(super) fn handle_session_history(
        &self,
        id: RequestId,
        params: Option<serde_json::Value>,
        transport: &AnyTransportWriter,
    ) -> JSONRPCResponse {
        let params: GetSessionHistoryParams =
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
            .history_page(transport.connection_id(), &params)
        {
            Ok(result) => {
                JSONRPCResponse::success(id, result).expect("Session history result serializes")
            }
            Err(error) => JSONRPCResponse::error(id, rpc_error(error)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use whale_protocol::MessagePhase;

    fn item(id: &str, text: &str) -> CanonicalItem {
        CanonicalItem::AssistantMessage {
            id: id.into(),
            content: vec![whale_protocol::CanonicalContent::Text { text: text.into() }],
            phase: MessagePhase::FinalAnswer,
        }
    }

    #[tokio::test]
    async fn count_and_byte_retention_advance_whole_item_floor() {
        let mut archive = HistoryArchive::with_limits("thread", "stream", 2, usize::MAX);
        archive
            .append(&[item("a", "a"), item("b", "b"), item("c", "c")])
            .unwrap();
        assert_eq!((archive.floor, archive.end), (1, 3));
        assert_eq!(archive.items.len(), 2);

        let one = usize::try_from(serialized_bytes(&item("d", "payload")).unwrap()).unwrap();
        let mut archive = HistoryArchive::with_limits("thread", "stream", 10, one);
        archive
            .append(&[item("d", "payload"), item("e", "payload")])
            .unwrap();
        assert_eq!((archive.floor, archive.end), (1, 2));
        assert_eq!(archive.items.front().unwrap().item.id(), "e");
        assert_eq!(archive.bytes, one);
    }

    #[tokio::test]
    async fn retained_history_returns_typed_gap_and_cursor_authentication_is_owner_bound() {
        let codec = CursorCodec::fixed_for_test([3; 32]);
        let mut archive = HistoryArchive::with_limits("thread", "stream", 2, usize::MAX);
        archive
            .append(&[item("a", "a"), item("b", "b"), item("c", "c")])
            .unwrap();
        let gap = archive.page(
            &codec,
            "owner-a",
            &GetSessionHistoryParams {
                thread_id: "thread".into(),
                before: Some(SessionHistoryAnchor {
                    thread_id: "thread".into(),
                    stream_id: "stream".into(),
                    index: 0,
                }),
                cursor: None,
                limit: 1,
            },
        );
        assert!(matches!(
            gap,
            Err(SessionManagementFailure::HistoryGap {
                requested: SessionHistoryAnchor { index: 0, .. },
                floor: SessionHistoryAnchor { index: 1, .. },
                current: SessionHistoryAnchor { index: 3, .. }
            })
        ));

        let first = archive
            .page(
                &codec,
                "owner-a",
                &GetSessionHistoryParams {
                    thread_id: "thread".into(),
                    before: None,
                    cursor: None,
                    limit: 1,
                },
            )
            .unwrap();
        let cursor = first.next_cursor.unwrap();
        let wrong_owner = archive.page(
            &codec,
            "owner-b",
            &GetSessionHistoryParams {
                thread_id: "thread".into(),
                before: None,
                cursor: Some(cursor.clone()),
                limit: 1,
            },
        );
        assert!(matches!(
            wrong_owner,
            Err(SessionManagementFailure::HistoryCursorInvalid)
        ));

        let mut forged = cursor.into_string().into_bytes();
        forged[0] = if forged[0] == b'A' { b'B' } else { b'A' };
        let forged = SessionHistoryPageCursor::new(String::from_utf8(forged).unwrap()).unwrap();
        let forged = archive.page(
            &codec,
            "owner-a",
            &GetSessionHistoryParams {
                thread_id: "thread".into(),
                before: None,
                cursor: Some(forged),
                limit: 1,
            },
        );
        assert!(matches!(
            forged,
            Err(SessionManagementFailure::HistoryCursorInvalid)
        ));
    }

    #[tokio::test]
    async fn explicit_anchor_ahead_of_current_end_clamps_to_current_end() {
        let codec = CursorCodec::fixed_for_test([4; 32]);
        let mut archive = HistoryArchive::with_limits("thread", "stream", 8, usize::MAX);
        archive.append(&[item("a", "a")]).unwrap();

        let result = archive.page(
            &codec,
            "owner",
            &GetSessionHistoryParams {
                thread_id: "thread".into(),
                before: Some(SessionHistoryAnchor {
                    thread_id: "thread".into(),
                    stream_id: "stream".into(),
                    index: 2,
                }),
                cursor: None,
                limit: 1,
            },
        );
        let result = result.unwrap();
        assert_eq!(result.start_index, 0);
        assert_eq!(result.end_index, 1);
        assert_eq!(result.through.index, 1);
        assert_eq!(result.current_end.index, 1);
        assert_eq!(result.items.len(), 1);
    }
}
