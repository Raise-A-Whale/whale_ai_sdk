use whale_sdk_rust::{
    AgentDefinition, InteractionRequest, InteractionSubscriptionOptions, InteractionWatchOptions,
    RunHandle, RuntimeOptions, SessionHistoryOptions, SessionListOptions,
    SessionManagementWatchOptions, SessionViewHandle, ToolContext, WhaleRuntime,
};

#[allow(dead_code)]
async fn application_chain() -> Result<(), Box<dyn std::error::Error>> {
    let embedded = WhaleRuntime::open(RuntimeOptions::embedded()).await?;
    let agent = embedded.agent(
        AgentDefinition::new("external-application", "configured-model"),
        Vec::new(),
    )?;
    let session = agent.create_session().await?;
    let _sessions = embedded
        .client()
        .list_sessions(SessionListOptions::default())
        .await?;
    let view: SessionViewHandle = session.session_view();
    let snapshot = view.snapshot().await?;
    let _history = view
        .history_page(SessionHistoryOptions::default())
        .await?;
    let watch = view
        .watch(SessionManagementWatchOptions::default())
        .await?;
    let _last_received = watch.events.last_received();
    drop(watch);
    let _metadata = session
        .replace_metadata(snapshot.summary.view_revision, Default::default())
        .await?;
    let run: RunHandle = session
        .start_turn("compile the public application chain")
        .await?;
    let _result = run.result().await?;
    let _closed = session.close().await?;
    let _shutdown = embedded.shutdown().await?;

    let managed = WhaleRuntime::open(RuntimeOptions::managed("whale-daemon")).await?;
    let _info = managed.info();
    let _shutdown = managed.shutdown().await?;
    Ok(())
}

#[allow(dead_code)]
async fn interaction_chain(
    runtime: &WhaleRuntime,
    context: ToolContext,
) -> Result<(), Box<dyn std::error::Error>> {
    let agent = runtime
        .agent(
            AgentDefinition::new("interactive-application", "configured-model"),
            Vec::new(),
        )?
        .with_interactions_enabled();
    assert!(agent.interactions_enabled());
    let session = agent.create_session().await?;
    let snapshot = session.interaction_snapshot().await?;
    let watch = session
        .watch_interactions(InteractionWatchOptions::default())
        .await?;
    let _last_received = watch.events.last_received();
    drop(watch);
    let stream = session
        .subscribe_interactions_from(
            snapshot.cursor,
            InteractionSubscriptionOptions::default(),
        )
        .await?;
    drop(stream);
    let run = session.start_turn("compile Interaction host APIs").await?;
    let _pending = run.pending_interactions().await?;
    let _resolved = run
        .respond_interaction("request-id", Default::default())
        .await?;
    let _resolved = runtime
        .client()
        .respond_interaction(session.id(), run.id(), "request-id", Default::default())
        .await?;
    let _response = context
        .request_interaction(
            InteractionRequest::new(
                "example.clarification",
                "Need input",
                Default::default(),
                None,
            )
            .map_err(std::io::Error::other)?,
        )
        .await?;
    Ok(())
}

fn main() {}
