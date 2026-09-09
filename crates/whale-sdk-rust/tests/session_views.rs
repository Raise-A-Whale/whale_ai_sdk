use std::{sync::Arc, time::Duration};
use tokio::sync::Notify;
use whale_core::{AgentEngine, ApprovalGate, ToolExecutionCoordinator, ToolRegistry};
use whale_daemon::DaemonServer;
use whale_protocol::{AgentStreamEvent, CanonicalItem, MessagePhase};
use whale_sdk_rust::{
    SessionEventPayload, SessionViewError, SessionWatchOptions, SubscriptionOptions, WhaleClient,
};

#[test]
fn subscription_options_are_checked_and_have_stable_defaults() {
    let defaults = SubscriptionOptions::default();
    assert_eq!(defaults.buffer_capacity(), 64);
    assert_eq!(defaults.replay_page_size(), 128);

    assert!(matches!(
        SubscriptionOptions::new(0, 1),
        Err(SessionViewError::Sdk(_))
    ));
    assert!(matches!(
        SubscriptionOptions::new(1, 0),
        Err(SessionViewError::Sdk(_))
    ));
    assert!(matches!(
        SubscriptionOptions::new(usize::MAX, 1),
        Err(SessionViewError::Sdk(_))
    ));
    assert!(matches!(
        SubscriptionOptions::new(1, 257),
        Err(SessionViewError::Sdk(_))
    ));

    let watch = SessionWatchOptions::default();
    assert_eq!(watch.history_limit(), 256);
    assert_eq!(watch.subscription().buffer_capacity(), 64);
    assert!(matches!(
        SessionWatchOptions::new(0, defaults.clone()),
        Err(SessionViewError::Sdk(_))
    ));
    assert!(matches!(
        SessionWatchOptions::new(1025, defaults),
        Err(SessionViewError::Sdk(_))
    ));
}

#[tokio::test]
async fn in_process_session_watchers_observe_one_run_independently_and_close_cleanly() {
    let release = Arc::new(Notify::new());
    let gate = Arc::new(ApprovalGate::new());
    let coordinator = Arc::new(ToolExecutionCoordinator::new(
        Arc::new(ToolRegistry::new()),
        gate.clone(),
    ));
    let engine = Arc::new(
        AgentEngine::new(coordinator).with_stream_provider(Arc::new({
            let release = release.clone();
            move |_, _| {
                let release = release.clone();
                Ok(Box::pin(async_stream::try_stream! {
                    release.notified().await;
                    yield AgentStreamEvent::ItemCompleted {
                        turn_id: "provider".into(),
                        item: CanonicalItem::assistant_text("done", MessagePhase::FinalAnswer),
                    };
                    yield AgentStreamEvent::TurnCompleted {
                        turn_id: "provider".into(),
                        thread_id: "provider".into(),
                        usage: Default::default(),
                    };
                }) as whale_adapters::BoxedEventStream)
            }
        })),
    );
    let client = WhaleClient::in_process(Arc::new(DaemonServer::new(engine, gate)));
    let thread = client.create_thread("mock", None).await.unwrap();
    let mut first = thread.watch().await.unwrap().events;
    let mut second = thread.watch().await.unwrap().events;

    let run = thread.start_turn("go").await.unwrap();
    for stream in [&mut first, &mut second] {
        let envelope = tokio::time::timeout(Duration::from_secs(2), stream.recv())
            .await
            .expect("watcher did not receive accepted Run")
            .unwrap()
            .unwrap();
        assert!(matches!(
            envelope.payload,
            SessionEventPayload::RunChanged { run: observed }
                if observed.snapshot.turn_id == run.id()
        ));
    }

    release.notify_one();
    tokio::time::timeout(Duration::from_secs(2), run.result())
        .await
        .expect("Run did not finish")
        .unwrap();
    assert!(thread.close().await.unwrap());
    assert!(first.recv().await.is_none());
    assert!(second.recv().await.is_none());
    client.close().await;
}
