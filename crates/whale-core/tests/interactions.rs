use async_trait::async_trait;
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::sync::{mpsc, oneshot, Semaphore};
use whale_core::{
    validate_interaction_request, validate_interaction_response, ApprovalDecision, ApprovalGate,
    CancellationToken, CoreError, InteractionBeginGuard, InteractionBridge, InteractionTicket,
    ToolContext, ToolExecutionCoordinator, ToolHandler, ToolRegistry,
};
use whale_protocol::{
    canonical::{CanonicalItem, CanonicalToolOutput},
    contexts::{RunContextInfo, ToolContextInfo},
    events::AgentStreamEvent,
    interactions::{
        InteractionRequest, INTERACTION_REMOVAL_PUBLICATION_FAILED, KIND_TOOL_APPROVAL,
    },
};

#[derive(Clone)]
struct Published {
    context: RunContextInfo,
    request_id: String,
    request: InteractionRequest,
}

struct PendingResponse {
    generation: usize,
    context: RunContextInfo,
    request: InteractionRequest,
    sender: Option<oneshot::Sender<Result<Value, CoreError>>>,
    committed: Option<Value>,
}

#[derive(Clone)]
struct ControlledBridge {
    next_generation: Arc<AtomicUsize>,
    pending: Arc<Mutex<HashMap<String, PendingResponse>>>,
    published: mpsc::UnboundedSender<Published>,
    cleanup: mpsc::UnboundedSender<(String, usize)>,
    publication_gate: Option<Arc<Semaphore>>,
    response_gate: Option<Arc<Semaphore>>,
    begin_error: Option<String>,
    clears: Arc<Mutex<Vec<(String, String)>>>,
}

impl ControlledBridge {
    fn new() -> (
        Self,
        mpsc::UnboundedReceiver<Published>,
        mpsc::UnboundedReceiver<(String, usize)>,
    ) {
        let (published, published_rx) = mpsc::unbounded_channel();
        let (cleanup, cleanup_rx) = mpsc::unbounded_channel();
        (
            Self {
                next_generation: Arc::new(AtomicUsize::new(0)),
                pending: Arc::new(Mutex::new(HashMap::new())),
                published,
                cleanup,
                publication_gate: None,
                response_gate: None,
                begin_error: None,
                clears: Arc::new(Mutex::new(Vec::new())),
            },
            published_rx,
            cleanup_rx,
        )
    }

    fn with_publication_gate(mut self, gate: Arc<Semaphore>) -> Self {
        self.publication_gate = Some(gate);
        self
    }

    fn with_begin_error(mut self, message: &str) -> Self {
        self.begin_error = Some(message.into());
        self
    }

    fn with_response_gate(mut self, gate: Arc<Semaphore>) -> Self {
        self.response_gate = Some(gate);
        self
    }

    fn resolve(&self, request_id: &str, response: Value) -> Result<(), CoreError> {
        let sender = {
            let mut pending = self.pending.lock().unwrap();
            let pending = pending.get_mut(request_id).ok_or_else(|| {
                CoreError::InteractionUnavailable("Interaction is not pending".into())
            })?;
            validate_interaction_response(&pending.request, &response)?;
            if let Some(committed) = &pending.committed {
                return if committed == &response {
                    Ok(())
                } else {
                    Err(CoreError::InteractionResponseInvalid(
                        "A different response already won this Interaction".into(),
                    ))
                };
            }
            pending.committed = Some(response.clone());
            pending.sender.take().ok_or_else(|| {
                CoreError::InteractionUnavailable("Interaction response sender is closed".into())
            })?
        };
        sender.send(Ok(response)).map_err(|_| {
            CoreError::InteractionCancelled("Interaction response waiter closed".into())
        })
    }

    fn pending_ids(&self) -> Vec<String> {
        let mut ids: Vec<_> = self.pending.lock().unwrap().keys().cloned().collect();
        ids.sort();
        ids
    }

    fn clears(&self) -> Vec<(String, String)> {
        self.clears.lock().unwrap().clone()
    }
}

#[async_trait]
impl InteractionBridge for ControlledBridge {
    async fn begin(
        &self,
        context: &RunContextInfo,
        request_id: String,
        request: InteractionRequest,
    ) -> Result<InteractionTicket, CoreError> {
        if let Some(message) = &self.begin_error {
            return Err(CoreError::InteractionUnavailable(message.clone()));
        }
        validate_interaction_request(&request)?;
        let generation = self.next_generation.fetch_add(1, Ordering::SeqCst);
        let (sender, response) = oneshot::channel();
        self.pending.lock().unwrap().insert(
            request_id.clone(),
            PendingResponse {
                generation,
                context: context.clone(),
                request: request.clone(),
                sender: Some(sender),
                committed: None,
            },
        );
        let pending = self.pending.clone();
        let cleanup = self.cleanup.clone();
        let cleanup_id = request_id.clone();
        let begin_guard = InteractionBeginGuard::new(request_id.clone(), move || {
            let removed = {
                let mut pending = pending.lock().unwrap();
                if pending
                    .get(&cleanup_id)
                    .is_some_and(|current| current.generation == generation)
                {
                    pending.remove(&cleanup_id);
                    true
                } else {
                    false
                }
            };
            if removed {
                let _ = cleanup.send((cleanup_id, generation));
            }
        });
        self.published
            .send(Published {
                context: context.clone(),
                request_id: request_id.clone(),
                request,
            })
            .unwrap();
        if let Some(gate) = &self.publication_gate {
            gate.acquire().await.unwrap().forget();
        }

        let response_gate = self.response_gate.clone();
        Ok(begin_guard.complete(async move {
            let response = response.await.unwrap_or_else(|_| {
                Err(CoreError::InteractionCancelled(
                    "Interaction response sender closed".into(),
                ))
            })?;
            if let Some(gate) = response_gate {
                gate.acquire()
                    .await
                    .map_err(|_| CoreError::InteractionCancelled("Response gate closed".into()))?
                    .forget();
            }
            Ok(response)
        }))
    }

    fn respond(
        &self,
        context: &RunContextInfo,
        request_id: &str,
        response: Value,
    ) -> Result<(), CoreError> {
        let pending_context = self
            .pending
            .lock()
            .unwrap()
            .get(request_id)
            .map(|pending| pending.context.clone())
            .ok_or_else(|| {
                CoreError::InteractionUnavailable("Interaction is not pending".into())
            })?;
        if &pending_context != context {
            return Err(CoreError::InteractionUnavailable(
                "Interaction belongs to another Run".into(),
            ));
        }
        self.resolve(request_id, response)
    }

    fn clear(&self, context: &RunContextInfo, request_id: &str, cause: &str) {
        if self
            .pending
            .lock()
            .unwrap()
            .get(request_id)
            .is_some_and(|pending| &pending.context == context)
        {
            self.clears
                .lock()
                .unwrap()
                .push((request_id.to_owned(), cause.to_owned()));
        }
    }
}

fn tool_context(turn_id: &str, cancellation: CancellationToken) -> ToolContext {
    ToolContext::new(
        ToolContextInfo {
            run: RunContextInfo {
                agent_name: Some("agent-a".into()),
                thread_id: "thread-a".into(),
                turn_id: turn_id.into(),
                deadline_unix_ms: None,
            },
            call_id: format!("model-call-{turn_id}"),
        },
        cancellation,
        None,
    )
}

#[tokio::test]
async fn custom_request_publishes_successfully_before_suspending() {
    let (bridge, mut published, _cleanup) = ControlledBridge::new();
    let gate = Arc::new(Semaphore::new(0));
    let bridge = Arc::new(bridge.with_publication_gate(gate.clone()));
    let context =
        tool_context("turn-a", CancellationToken::new()).with_interaction_bridge(bridge.clone());
    let task = tokio::spawn(async move {
        context
            .request_interaction(
                InteractionRequest::new("acme.review", "Review", json!({"preview":"safe"}), None)
                    .unwrap(),
            )
            .await
    });

    let publication = published.recv().await.unwrap();
    assert_eq!(publication.context.turn_id, "turn-a");
    assert_eq!(publication.request.kind, "acme.review");
    assert!(!task.is_finished());
    gate.add_permits(1);
    tokio::task::yield_now().await;
    assert!(!task.is_finished());

    assert!(bridge
        .resolve(&publication.request_id, json!({"decision":"continue"}))
        .is_ok());
    assert_eq!(task.await.unwrap().unwrap(), json!({"decision":"continue"}));
}

#[tokio::test]
async fn cancellation_cleans_only_the_exact_origin_and_wakes_its_waiter() {
    let (bridge, mut published, mut cleanup) = ControlledBridge::new();
    let bridge = Arc::new(bridge);
    let cancellation_a = CancellationToken::new();
    let cancellation_b = CancellationToken::new();
    let context_a =
        tool_context("turn-a", cancellation_a.clone()).with_interaction_bridge(bridge.clone());
    let context_b =
        tool_context("turn-b", cancellation_b.clone()).with_interaction_bridge(bridge.clone());
    let request = || InteractionRequest::new("acme.form", "Form", json!({}), None).unwrap();

    let task_a = tokio::spawn(async move { context_a.request_interaction(request()).await });
    let task_b = tokio::spawn(async move { context_b.request_interaction(request()).await });
    let first = published.recv().await.unwrap();
    let second = published.recv().await.unwrap();
    let (published_a, published_b) = if first.context.turn_id == "turn-a" {
        (first, second)
    } else {
        (second, first)
    };

    cancellation_a.cancel();
    let error = tokio::time::timeout(Duration::from_millis(200), task_a)
        .await
        .expect("cancelled Interaction waiter did not wake")
        .unwrap()
        .unwrap_err();
    assert!(matches!(error, CoreError::InteractionCancelled(_)));
    let (cleaned_id, _) = cleanup.recv().await.unwrap();
    assert_eq!(cleaned_id, published_a.request_id);
    assert_eq!(bridge.pending_ids(), vec![published_b.request_id.clone()]);
    assert!(!task_b.is_finished());

    assert!(bridge
        .resolve(&published_b.request_id, json!({"answer":"ok"}))
        .is_ok());
    assert_eq!(task_b.await.unwrap().unwrap(), json!({"answer":"ok"}));
}

#[tokio::test]
async fn run_cancellation_wakes_and_cleans_every_waiter_from_that_origin_only() {
    let (bridge, mut published, mut cleanup) = ControlledBridge::new();
    let bridge = Arc::new(bridge);
    let cancellation_a = CancellationToken::new();
    let cancellation_b = CancellationToken::new();
    let context_a_one =
        tool_context("turn-a", cancellation_a.clone()).with_interaction_bridge(bridge.clone());
    let context_a_two =
        tool_context("turn-a", cancellation_a.clone()).with_interaction_bridge(bridge.clone());
    let context_b = tool_context("turn-b", cancellation_b).with_interaction_bridge(bridge.clone());

    let task_a_one = tokio::spawn(async move {
        context_a_one
            .request_interaction(
                InteractionRequest::new("acme.form", "First", json!({}), None).unwrap(),
            )
            .await
    });
    let task_a_two = tokio::spawn(async move {
        context_a_two
            .request_interaction(
                InteractionRequest::new("acme.form", "Second", json!({}), None).unwrap(),
            )
            .await
    });
    let task_b = tokio::spawn(async move {
        context_b
            .request_interaction(
                InteractionRequest::new("acme.form", "Other", json!({}), None).unwrap(),
            )
            .await
    });

    let mut origin_a_ids = Vec::new();
    let mut origin_b_id = None;
    for _ in 0..3 {
        let publication = published.recv().await.unwrap();
        if publication.context.turn_id == "turn-a" {
            origin_a_ids.push(publication.request_id);
        } else {
            origin_b_id = Some(publication.request_id);
        }
    }
    origin_a_ids.sort();
    let origin_b_id = origin_b_id.unwrap();

    cancellation_a.cancel();
    for task in [task_a_one, task_a_two] {
        let error = tokio::time::timeout(Duration::from_millis(200), task)
            .await
            .expect("cancelled Interaction waiter did not wake")
            .unwrap()
            .unwrap_err();
        assert!(matches!(error, CoreError::InteractionCancelled(_)));
    }

    let mut cleaned_ids = vec![
        cleanup.recv().await.unwrap().0,
        cleanup.recv().await.unwrap().0,
    ];
    cleaned_ids.sort();
    assert_eq!(cleaned_ids, origin_a_ids);
    assert_eq!(bridge.pending_ids(), vec![origin_b_id.clone()]);
    assert!(!task_b.is_finished());

    assert!(bridge.resolve(&origin_b_id, json!({"answer":"ok"})).is_ok());
    assert_eq!(task_b.await.unwrap().unwrap(), json!({"answer":"ok"}));
}

#[tokio::test]
async fn finishing_tool_origin_wakes_cloned_context_waiter_without_touching_another_origin() {
    let (bridge, mut published, mut cleanup) = ControlledBridge::new();
    let bridge = Arc::new(bridge);
    let context_a =
        tool_context("turn-a", CancellationToken::new()).with_interaction_bridge(bridge.clone());
    let waiter_a = context_a.clone();
    let context_b =
        tool_context("turn-b", CancellationToken::new()).with_interaction_bridge(bridge.clone());

    let task_a = tokio::spawn(async move {
        waiter_a
            .request_interaction(
                InteractionRequest::new("acme.form", "First", json!({}), None).unwrap(),
            )
            .await
    });
    let task_b = tokio::spawn(async move {
        context_b
            .request_interaction(
                InteractionRequest::new("acme.form", "Other", json!({}), None).unwrap(),
            )
            .await
    });

    let first = published.recv().await.unwrap();
    let second = published.recv().await.unwrap();
    let (origin_a_id, origin_b_id) = if first.context.turn_id == "turn-a" {
        (first.request_id, second.request_id)
    } else {
        (second.request_id, first.request_id)
    };

    context_a.finish();
    let error = tokio::time::timeout(Duration::from_millis(200), task_a)
        .await
        .expect("finished tool origin did not wake cloned Interaction waiter")
        .unwrap()
        .unwrap_err();
    assert!(matches!(error, CoreError::InteractionCancelled(_)));
    assert_eq!(cleanup.recv().await.unwrap().0, origin_a_id);
    assert_eq!(bridge.pending_ids(), vec![origin_b_id.clone()]);
    assert!(!task_b.is_finished());

    assert!(bridge.resolve(&origin_b_id, json!({"answer":"ok"})).is_ok());
    assert_eq!(task_b.await.unwrap().unwrap(), json!({"answer":"ok"}));
}

#[tokio::test]
async fn dropping_ticket_runs_exact_cleanup_once() {
    let (bridge, mut published, mut cleanup) = ControlledBridge::new();
    let context = RunContextInfo {
        agent_name: None,
        thread_id: "thread-a".into(),
        turn_id: "turn-a".into(),
        deadline_unix_ms: None,
    };
    let ticket = bridge
        .begin(
            &context,
            "request-a".into(),
            InteractionRequest::new("acme.kind", "Title", json!({}), None).unwrap(),
        )
        .await
        .unwrap();
    published.recv().await.unwrap();
    drop(ticket);
    assert_eq!(cleanup.recv().await.unwrap().0, "request-a");
    assert!(bridge.pending_ids().is_empty());
    assert!(cleanup.try_recv().is_err());
}

#[tokio::test]
async fn stale_ticket_drop_does_not_remove_replacement_generation() {
    let (bridge, mut published, mut cleanup) = ControlledBridge::new();
    let context = RunContextInfo {
        agent_name: None,
        thread_id: "thread-a".into(),
        turn_id: "turn-a".into(),
        deadline_unix_ms: None,
    };
    let first = bridge
        .begin(
            &context,
            "same-request".into(),
            InteractionRequest::new("acme.kind", "First", json!({}), None).unwrap(),
        )
        .await
        .unwrap();
    published.recv().await.unwrap();
    let second = bridge
        .begin(
            &context,
            "same-request".into(),
            InteractionRequest::new("acme.kind", "Second", json!({}), None).unwrap(),
        )
        .await
        .unwrap();
    published.recv().await.unwrap();

    drop(first);
    assert_eq!(bridge.pending_ids(), vec!["same-request"]);
    assert!(cleanup.try_recv().is_err());

    drop(second);
    assert_eq!(cleanup.recv().await.unwrap(), ("same-request".into(), 1));
    assert!(bridge.pending_ids().is_empty());
    assert!(cleanup.try_recv().is_err());
}

#[tokio::test]
async fn bridge_publication_failure_returns_without_suspending() {
    let (bridge, _published, _cleanup) = ControlledBridge::new();
    let bridge = Arc::new(bridge.with_begin_error("publication failed"));
    let context = tool_context("turn-a", CancellationToken::new()).with_interaction_bridge(bridge);
    let error = context
        .request_interaction(
            InteractionRequest::new("acme.kind", "Title", json!({}), None).unwrap(),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(error, CoreError::InteractionUnavailable(message) if message == "publication failed")
    );
}

#[tokio::test]
async fn run_cancellation_during_begin_cleans_provisional_identity() {
    let (bridge, mut published, mut cleanup) = ControlledBridge::new();
    let publication_gate = Arc::new(Semaphore::new(0));
    let bridge = Arc::new(bridge.with_publication_gate(publication_gate));
    let cancellation = CancellationToken::new();
    let context =
        tool_context("turn-a", cancellation.clone()).with_interaction_bridge(bridge.clone());
    let task = tokio::spawn(async move {
        context
            .request_interaction(
                InteractionRequest::new("acme.form", "Form", json!({}), None).unwrap(),
            )
            .await
    });

    let publication = published.recv().await.unwrap();
    cancellation.cancel();
    let error = tokio::time::timeout(Duration::from_millis(200), task)
        .await
        .expect("Run cancellation did not interrupt begin")
        .unwrap()
        .unwrap_err();
    assert!(matches!(error, CoreError::InteractionCancelled(_)));
    assert_eq!(cleanup.recv().await.unwrap().0, publication.request_id);
    assert!(bridge.pending_ids().is_empty());
    assert!(cleanup.try_recv().is_err());
}

#[tokio::test]
async fn origin_finish_during_begin_cleans_provisional_identity() {
    let (bridge, mut published, mut cleanup) = ControlledBridge::new();
    let publication_gate = Arc::new(Semaphore::new(0));
    let bridge = Arc::new(bridge.with_publication_gate(publication_gate));
    let context =
        tool_context("turn-a", CancellationToken::new()).with_interaction_bridge(bridge.clone());
    let waiter = context.clone();
    let task = tokio::spawn(async move {
        waiter
            .request_interaction(
                InteractionRequest::new("acme.form", "Form", json!({}), None).unwrap(),
            )
            .await
    });

    let publication = published.recv().await.unwrap();
    context.finish();
    let error = tokio::time::timeout(Duration::from_millis(200), task)
        .await
        .expect("origin finish did not interrupt begin")
        .unwrap()
        .unwrap_err();
    assert!(matches!(error, CoreError::InteractionCancelled(_)));
    assert_eq!(cleanup.recv().await.unwrap().0, publication.request_id);
    assert!(bridge.pending_ids().is_empty());
    assert!(cleanup.try_recv().is_err());
}

struct MismatchedTicketBridge {
    cleanup_calls: Arc<AtomicUsize>,
}

#[async_trait]
impl InteractionBridge for MismatchedTicketBridge {
    async fn begin(
        &self,
        _context: &RunContextInfo,
        _request_id: String,
        _request: InteractionRequest,
    ) -> Result<InteractionTicket, CoreError> {
        let cleanup_calls = self.cleanup_calls.clone();
        Ok(InteractionTicket::new(
            "another-request".into(),
            std::future::pending(),
            move || {
                cleanup_calls.fetch_add(1, Ordering::SeqCst);
            },
        ))
    }
}

#[tokio::test]
async fn mismatched_ticket_identity_is_rejected_and_cleaned_once() {
    let cleanup_calls = Arc::new(AtomicUsize::new(0));
    let bridge = Arc::new(MismatchedTicketBridge {
        cleanup_calls: cleanup_calls.clone(),
    });
    let context = tool_context("turn-a", CancellationToken::new()).with_interaction_bridge(bridge);
    let error = tokio::time::timeout(
        Duration::from_millis(200),
        context.request_interaction(
            InteractionRequest::new("acme.form", "Form", json!({}), None).unwrap(),
        ),
    )
    .await
    .expect("mismatched ticket was incorrectly awaited")
    .unwrap_err();
    assert!(matches!(error, CoreError::InteractionUnavailable(_)));
    assert_eq!(cleanup_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn direct_bridge_rejects_invalid_response_without_consuming_pending_request() {
    let (bridge, mut published, _cleanup) = ControlledBridge::new();
    let bridge = Arc::new(bridge);
    let context =
        tool_context("turn-a", CancellationToken::new()).with_interaction_bridge(bridge.clone());
    let task = tokio::spawn(async move {
        context
            .request_interaction(
                InteractionRequest::new(
                    "acme.form",
                    "Form",
                    json!({}),
                    Some(json!({
                        "$schema":"https://json-schema.org/draft/2020-12/schema",
                        "type":"object",
                        "properties":{"answer":{"type":"string"}},
                        "required":["answer"],
                        "additionalProperties":false
                    })),
                )
                .unwrap(),
            )
            .await
    });
    let publication = published.recv().await.unwrap();

    let oversized =
        Value::String("x".repeat(whale_protocol::interactions::MAX_INTERACTION_RESPONSE_BYTES));
    assert!(matches!(
        bridge.resolve(&publication.request_id, oversized),
        Err(CoreError::InteractionResponseInvalid(_))
    ));
    assert_eq!(bridge.pending_ids(), vec![publication.request_id.clone()]);
    assert!(matches!(
        bridge.resolve(&publication.request_id, json!({"answer":7})),
        Err(CoreError::InteractionResponseInvalid(_))
    ));
    assert_eq!(bridge.pending_ids(), vec![publication.request_id.clone()]);

    bridge
        .resolve(&publication.request_id, json!({"answer":"valid"}))
        .unwrap();
    assert_eq!(task.await.unwrap().unwrap(), json!({"answer":"valid"}));
}

#[tokio::test]
async fn malformed_response_schema_is_rejected_before_bridge_publication() {
    let (bridge, mut published, _cleanup) = ControlledBridge::new();
    let bridge = Arc::new(bridge.with_begin_error("begin must not be called"));
    let context = tool_context("turn-a", CancellationToken::new()).with_interaction_bridge(bridge);
    let request =
        InteractionRequest::new("acme.form", "Form", json!({}), Some(json!({"type":7}))).unwrap();
    let error = context.request_interaction(request).await.unwrap_err();
    assert!(matches!(error, CoreError::InteractionRequestInvalid(_)));
    assert!(published.try_recv().is_err());
}

#[test]
fn response_validation_errors_do_not_echo_sensitive_values() {
    let request = InteractionRequest::new(
        "whale.auth",
        "Authentication",
        json!({"provider":"example"}),
        Some(json!({
            "$schema":"https://json-schema.org/draft/2020-12/schema",
            "type":"object",
            "properties":{"credential":{"type":"integer"}},
            "required":["credential"]
        })),
    )
    .unwrap();
    let error = validate_interaction_response(
        &request,
        &json!({"credential":"super-secret-validation-value"}),
    )
    .unwrap_err();
    assert!(matches!(error, CoreError::InteractionResponseInvalid(_)));
    assert!(!error.to_string().contains("super-secret-validation-value"));
}

#[tokio::test]
async fn direct_core_approval_behavior_is_unchanged_without_bridge() {
    let gate = Arc::new(ApprovalGate::new());
    let (events, mut event_rx) = mpsc::channel(4);
    let tool_call = CanonicalItem::tool_call(
        "call-a",
        None,
        "sensitive",
        Some(json!({"query":"original"})),
        r#"{"query":"original"}"#,
    );
    let request = {
        let gate = gate.clone();
        let tool_call = tool_call.clone();
        tokio::spawn(async move {
            gate.request_approval("turn-a", &tool_call, Some("reason".into()), &events)
                .await
        })
    };
    let AgentStreamEvent::ApprovalRequested { request_id, .. } = event_rx.recv().await.unwrap()
    else {
        panic!("expected typed approval event");
    };
    assert_eq!(gate.pending_count(), 1);
    assert!(gate.resolve_approval(&request_id, ApprovalDecision::Accept));
    assert_eq!(request.await.unwrap().unwrap(), ApprovalDecision::Accept);
    assert_eq!(gate.pending_count(), 0);
}

struct ApprovalTool {
    calls: Arc<AtomicUsize>,
    arguments: Arc<Mutex<Vec<Value>>>,
}

#[async_trait]
impl ToolHandler for ApprovalTool {
    fn name(&self) -> &str {
        "sensitive"
    }

    fn description(&self) -> &str {
        "sensitive"
    }

    fn parameters(&self) -> Value {
        json!({
            "type":"object",
            "properties":{"query":{"type":"string","minLength":2}},
            "required":["query"],
            "additionalProperties":false
        })
    }

    fn require_approval(&self) -> bool {
        true
    }

    async fn execute(&self, arguments: Value) -> Result<CanonicalToolOutput, String> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.arguments.lock().unwrap().push(arguments);
        Ok(CanonicalToolOutput::text("done"))
    }
}

fn approval_fixture(
    bridge: Arc<ControlledBridge>,
) -> (
    ToolExecutionCoordinator,
    Arc<ApprovalGate>,
    Arc<AtomicUsize>,
    Arc<Mutex<Vec<Value>>>,
) {
    let calls = Arc::new(AtomicUsize::new(0));
    let arguments = Arc::new(Mutex::new(Vec::new()));
    let registry = Arc::new(ToolRegistry::new());
    registry
        .register(Arc::new(ApprovalTool {
            calls: calls.clone(),
            arguments: arguments.clone(),
        }))
        .unwrap();
    let gate = Arc::new(ApprovalGate::new().with_interaction_bridge(bridge));
    (
        ToolExecutionCoordinator::new(registry, gate.clone()),
        gate,
        calls,
        arguments,
    )
}

fn approval_call(arguments: Value) -> CanonicalItem {
    CanonicalItem::tool_call(
        "call-a",
        None,
        "sensitive",
        Some(arguments),
        r#"{"query":"original"}"#,
    )
}

async fn execute_approval(
    coordinator: ToolExecutionCoordinator,
    arguments: Value,
) -> (Vec<CanonicalItem>, mpsc::Receiver<AgentStreamEvent>) {
    let (events, event_rx) = mpsc::channel(16);
    let result = coordinator
        .execute_calls_with_context(
            RunContextInfo {
                agent_name: None,
                thread_id: "thread-a".into(),
                turn_id: "turn-a".into(),
                deadline_unix_ms: None,
            },
            CancellationToken::new(),
            Arc::new(whale_core::engine::RunProgress::default()),
            vec![approval_call(arguments)],
            None,
            Some(events),
        )
        .await;
    (result, event_rx)
}

async fn wait_for_pending_approval(gate: &ApprovalGate) {
    tokio::time::timeout(Duration::from_millis(200), async {
        while gate.pending_count() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("typed approval was not published");
}

#[tokio::test]
async fn typed_event_publication_failure_clears_bridge_identity_with_exact_cause() {
    let (bridge, mut published, mut cleanup) = ControlledBridge::new();
    let bridge = Arc::new(bridge);
    let (coordinator, _gate, calls, _) = approval_fixture(bridge.clone());
    let (events, receiver) = mpsc::channel(1);
    drop(receiver);

    let results = coordinator
        .execute_calls_with_context(
            RunContextInfo {
                agent_name: None,
                thread_id: "thread-a".into(),
                turn_id: "turn-a".into(),
                deadline_unix_ms: None,
            },
            CancellationToken::new(),
            Arc::new(whale_core::engine::RunProgress::default()),
            vec![approval_call(json!({"query":"original"}))],
            None,
            Some(events),
        )
        .await;

    let request = published.recv().await.unwrap();
    assert_eq!(
        bridge.clears(),
        vec![(
            request.request_id.clone(),
            INTERACTION_REMOVAL_PUBLICATION_FAILED.into()
        )]
    );
    assert_eq!(cleanup.recv().await.unwrap().0, request.request_id);
    assert!(bridge.pending_ids().is_empty());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(matches!(
        results.first(),
        Some(CanonicalItem::ToolResult { is_error: true, .. })
    ));
}

#[tokio::test]
async fn generic_reject_committed_first_cannot_be_overridden_by_legacy_accept() {
    let (bridge, mut published, _cleanup) = ControlledBridge::new();
    let response_gate = Arc::new(Semaphore::new(0));
    let bridge = Arc::new(bridge.with_response_gate(response_gate.clone()));
    let (coordinator, gate, calls, _) = approval_fixture(bridge.clone());
    let execution = tokio::spawn(execute_approval(coordinator, json!({"query":"original"})));
    let publication = published.recv().await.unwrap();
    wait_for_pending_approval(&gate).await;

    bridge
        .resolve(
            &publication.request_id,
            json!({"decision":"reject","feedback":"denied"}),
        )
        .unwrap();
    assert!(!gate.resolve_approval(&publication.request_id, ApprovalDecision::Accept));
    response_gate.add_permits(1);

    let (results, _) = execution.await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(matches!(
        &results[0],
        CanonicalItem::ToolResult { is_error: true, .. }
    ));
}

#[tokio::test]
async fn legacy_accept_committed_first_rejects_later_generic_reject() {
    let (bridge, mut published, _cleanup) = ControlledBridge::new();
    let response_gate = Arc::new(Semaphore::new(0));
    let bridge = Arc::new(bridge.with_response_gate(response_gate.clone()));
    let (coordinator, gate, calls, _) = approval_fixture(bridge.clone());
    let execution = tokio::spawn(execute_approval(coordinator, json!({"query":"original"})));
    let publication = published.recv().await.unwrap();
    wait_for_pending_approval(&gate).await;

    assert!(gate.resolve_approval(&publication.request_id, ApprovalDecision::Accept));
    assert!(matches!(
        bridge.resolve(
            &publication.request_id,
            json!({"decision":"reject","feedback":"too late"})
        ),
        Err(CoreError::InteractionResponseInvalid(_))
    ));
    response_gate.add_permits(1);

    let (_results, _) = execution.await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn typed_approval_maps_approve_reject_and_modified_arguments() {
    for (response, expected_calls, expected_arguments) in [
        (
            json!({"decision":"approve"}),
            1,
            Some(json!({"query":"original"})),
        ),
        (json!({"decision":"reject","feedback":"no"}), 0, None),
        (
            json!({"decision":"modify_arguments","arguments":{"query":"changed"}}),
            1,
            Some(json!({"query":"changed"})),
        ),
    ] {
        let (bridge, mut published, _cleanup) = ControlledBridge::new();
        let bridge = Arc::new(bridge);
        let (coordinator, _gate, calls, arguments) = approval_fixture(bridge.clone());
        let execution = tokio::spawn(execute_approval(coordinator, json!({"query":"original"})));
        let publication = published.recv().await.unwrap();
        assert_eq!(publication.request.kind, KIND_TOOL_APPROVAL);
        assert_eq!(
            publication.request.payload["tool_call"]["call_id"],
            "call-a"
        );
        assert!(bridge.resolve(&publication.request_id, response).is_ok());

        let (results, mut events) = execution.await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), expected_calls);
        assert_eq!(
            arguments.lock().unwrap().as_slice(),
            expected_arguments.as_slice()
        );
        assert_eq!(results.len(), 1);
        assert!(matches!(results[0], CanonicalItem::ToolResult { .. }));
        let mut typed_request_id = None;
        while let Some(event) = events.recv().await {
            if let AgentStreamEvent::ApprovalRequested { request_id, .. } = event {
                typed_request_id = Some(request_id);
            }
        }
        assert_eq!(
            typed_request_id.as_deref(),
            Some(publication.request_id.as_str())
        );
    }
}

#[tokio::test]
async fn approval_validates_before_publication_and_after_argument_replacement() {
    let (bridge, mut published, _cleanup) = ControlledBridge::new();
    let bridge = Arc::new(bridge);
    let (coordinator, _gate, calls, _) = approval_fixture(bridge.clone());
    let (invalid_initial, _) = execute_approval(coordinator, json!({"query":"x"})).await;
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(published.try_recv().is_err());
    assert!(matches!(
        &invalid_initial[0],
        CanonicalItem::ToolResult { is_error: true, .. }
    ));

    let (coordinator, _gate, calls, _) = approval_fixture(bridge.clone());
    let execution = tokio::spawn(execute_approval(coordinator, json!({"query":"original"})));
    let publication = published.recv().await.unwrap();
    assert!(bridge
        .resolve(
            &publication.request_id,
            json!({"decision":"modify_arguments","arguments":{"query":"x"}})
        )
        .is_ok());
    let (invalid_modified, _) = execution.await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(matches!(
        &invalid_modified[0],
        CanonicalItem::ToolResult { is_error: true, .. }
    ));
}
