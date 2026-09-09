//! Per-connection bootstrap. Readiness belongs to the delivered acknowledgement,
//! not merely to successful negotiation.
use super::*;
use whale_protocol::initialization::*;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    New,
    Initializing,
    Ready,
    Failed,
    Closed,
}

#[derive(Default)]
pub(super) struct Initializations {
    connections: DashMap<String, watch::Sender<Phase>>,
}

pub(super) struct InitializationAck(watch::Sender<Phase>);
impl InitializationAck {
    pub(super) fn delivered(&self) {
        self.0.send_if_modified(|phase| {
            if *phase == Phase::Initializing {
                *phase = Phase::Ready;
                true
            } else {
                false
            }
        });
    }
}
impl Drop for InitializationAck {
    fn drop(&mut self) {
        self.0.send_if_modified(|phase| {
            if *phase == Phase::Initializing {
                *phase = Phase::Failed;
                true
            } else {
                false
            }
        });
    }
}
impl Initializations {
    fn state(&self, owner: &str) -> watch::Sender<Phase> {
        self.connections
            .entry(owner.to_owned())
            .or_insert_with(|| watch::channel(Phase::New).0)
            .clone()
    }
    pub(super) fn close(&self, owner: &str) {
        self.state(owner).send_replace(Phase::Closed);
    }
    pub(super) async fn ready(&self, owner: &str) -> Result<(), JSONRPCError> {
        let mut state = self.state(owner).subscribe();
        loop {
            let phase = *state.borrow_and_update();
            match phase {
                Phase::Ready => return Ok(()),
                Phase::Initializing => {
                    if state.changed().await.is_err() {
                        return Err(JSONRPCError::internal_error("ConnectionClosed"));
                    }
                }
                Phase::New => {
                    return Err(JSONRPCError::new(
                        PROTOCOL_NOT_INITIALIZED,
                        "ProtocolNotInitialized",
                        None,
                    ))
                }
                Phase::Failed | Phase::Closed => {
                    return Err(JSONRPCError::internal_error("ConnectionClosed"))
                }
            }
        }
    }
    pub(super) fn begin(
        &self,
        req: JSONRPCRequest,
        owner: &str,
        recovery: bool,
        retention: bool,
    ) -> (JSONRPCResponse, Option<InitializationAck>) {
        let state = self.state(owner);
        let reject = |error| (JSONRPCResponse::error(req.id.clone(), error), None);
        if *state.borrow() != Phase::New {
            return reject(JSONRPCError::new(
                PROTOCOL_ALREADY_INITIALIZED,
                "ProtocolAlreadyInitialized",
                None,
            ));
        }
        let params: InitializeParams =
            match serde_json::from_value(req.params.clone().unwrap_or(Value::Null)) {
                Ok(params) => params,
                Err(error) => return reject(JSONRPCError::invalid_params(error.to_string())),
            };
        if let Err(error) = params.validate() {
            return reject(JSONRPCError::invalid_params(error));
        }
        let mut capabilities = vec![whale_protocol::retention::CAPABILITY_SESSION_LIMITS];
        capabilities.extend([
            whale_protocol::session_views::CAPABILITY_SESSION_VIEWS,
            whale_protocol::session_views::CAPABILITY_SESSION_EVENT_REPLAY,
            whale_protocol::session_management::CAPABILITY_SESSION_CATALOG,
            whale_protocol::session_management::CAPABILITY_SESSION_HISTORY,
            whale_protocol::session_management::CAPABILITY_SESSION_METADATA_CAS,
            whale_protocol::session_management::CAPABILITY_SESSION_LIFECYCLE_REPLAY,
            whale_protocol::interactions::CAPABILITY_INTERACTIONS,
        ]);
        if retention {
            capabilities.push(whale_protocol::retention::CAPABILITY_RUN_RETENTION);
        }
        let mut baseline = params.clone();
        baseline
            .required_capabilities
            .retain(|cap| !capabilities.contains(&cap.as_str()));
        if recovery {
            baseline
                .required_capabilities
                .retain(|cap| cap != whale_protocol::recovery::CAPABILITY_SESSION_RECOVERY);
        }
        let mut result = match InitializeResult::negotiate(
            &baseline,
            PeerInfo {
                name: "whale-daemon".into(),
                version: env!("CARGO_PKG_VERSION").into(),
            },
        ) {
            Ok(result) => result,
            Err(error) => return reject(JSONRPCError::new(PROTOCOL_INCOMPATIBLE, error, None)),
        };
        if recovery {
            result
                .capabilities
                .push(whale_protocol::recovery::CAPABILITY_SESSION_RECOVERY.into());
        }
        result
            .capabilities
            .extend(capabilities.into_iter().map(str::to_owned));
        if let Err(error) = result.validate_for(&params) {
            return reject(JSONRPCError::new(PROTOCOL_INCOMPATIBLE, error, None));
        }
        let accepted = state.send_if_modified(|phase| {
            if *phase == Phase::New {
                *phase = Phase::Initializing;
                true
            } else {
                false
            }
        });
        if !accepted {
            return reject(JSONRPCError::new(
                PROTOCOL_ALREADY_INITIALIZED,
                "ProtocolAlreadyInitialized",
                None,
            ));
        }
        (
            JSONRPCResponse::success(req.id, result).expect("initialization result serializes"),
            Some(InitializationAck(state)),
        )
    }
}
