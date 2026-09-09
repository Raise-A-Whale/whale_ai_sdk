use async_trait::async_trait;
use serde_json::{json, Value};
use std::sync::Arc;
use whale_core::model::{ModelError, ModelEventStream, ModelProvider, ModelRequest};
use whale_core::provider::ProviderRegistry;
use whale_core::CancellationToken;
use whale_daemon::DaemonServer;
use whale_protocol::models::ModelCapabilities;
use whale_protocol::CanonicalToolOutput;
use whale_sdk_rust::{
    AgentDefinition, BoundToolPack, CancellationSignal, ContextBuildRequest, HostContextPolicy,
    HostTool, ModelContext, RuntimeOptions, RuntimeSource, SessionBindContext, ToolPack,
    ToolPackError, ToolPackManifest, ToolPackTool, WhaleRuntime,
};
use whale_store::{MemoryStore, SQLiteStore, SessionStore, StoreRuntime};

struct EmptyProvider;

#[async_trait]
impl ModelProvider for EmptyProvider {
    fn capabilities(&self, _model: &str) -> Result<ModelCapabilities, ModelError> {
        Ok(ModelCapabilities::text_only())
    }

    async fn stream(
        &self,
        _request: ModelRequest,
        _cancellation: CancellationToken,
    ) -> Result<ModelEventStream, ModelError> {
        Ok(Box::pin(futures::stream::empty()))
    }
}

struct PassthroughContext;

#[async_trait]
impl HostContextPolicy for PassthroughContext {
    async fn build(
        &self,
        request: ContextBuildRequest,
        _cancellation: CancellationSignal,
    ) -> Result<ModelContext, String> {
        Ok(ModelContext {
            system_prompt: request.system_prompt,
            items: request.history,
        })
    }
}

struct ExtensionTool;

#[async_trait]
impl HostTool for ExtensionTool {
    fn name(&self) -> &str {
        "extension_probe"
    }

    fn description(&self) -> &str {
        "Compile-check an extension-owned host tool"
    }

    fn parameters(&self) -> Value {
        json!({"type": "object"})
    }

    async fn execute(&self, _arguments: Value) -> Result<CanonicalToolOutput, String> {
        Ok(CanonicalToolOutput::text("extension available"))
    }
}

struct ExtensionBinding;

#[async_trait]
impl BoundToolPack for ExtensionBinding {
    fn tools(&self) -> Vec<Arc<dyn HostTool>> {
        vec![Arc::new(ExtensionTool)]
    }

    async fn close(&mut self) -> Result<(), ToolPackError> {
        Ok(())
    }

    fn emergency_close(&mut self) {}
}

struct ExtensionPack;

#[async_trait]
impl ToolPack for ExtensionPack {
    fn manifest(&self) -> ToolPackManifest {
        ToolPackManifest {
            id: "extension-pack".into(),
            tools: vec![ToolPackTool {
                name: "extension_probe".into(),
                description: "Compile-check an extension-owned host tool".into(),
                parameters: json!({"type": "object"}),
                supports_parallel: true,
                require_approval: false,
            }],
        }
    }

    async fn bind(
        &self,
        _context: SessionBindContext,
    ) -> Result<Box<dyn BoundToolPack>, ToolPackError> {
        Ok(Box::new(ExtensionBinding))
    }
}

#[allow(dead_code)]
async fn extension_chain() -> Result<(), Box<dyn std::error::Error>> {
    let mut providers = ProviderRegistry::new();
    providers.register_provider("external-provider", Arc::new(EmptyProvider))?;

    let memory_store: Arc<dyn SessionStore> = Arc::new(MemoryStore::new());
    let memory_runtime = Arc::new(StoreRuntime::open(memory_store).await?);

    // The durable implementation is owned by whale-store and requires its
    // explicit `sqlite` feature; constructing it is compile-checked, not run.
    let sqlite_store: Arc<dyn SessionStore> = Arc::new(SQLiteStore::open("compile-only.sqlite")?);
    let _sqlite_runtime = StoreRuntime::open(sqlite_store).await?;

    let server = DaemonServer::default_server()
        .with_provider_registry(Arc::new(providers))
        .with_store_runtime(memory_runtime);
    let mut options = RuntimeOptions::embedded();
    options.source = RuntimeSource::Embedded { server };
    let runtime = WhaleRuntime::open(options).await?;

    let mut definition = AgentDefinition::new("extension-agent", "external-model");
    definition.provider_ref = Some("external-provider".into());
    definition.tool_names = vec!["extension_probe".into()];
    let agent = runtime
        .agent_with_tool_packs(definition, Vec::new(), vec![Arc::new(ExtensionPack)])?
        .with_context_policy(Arc::new(PassthroughContext));

    let session = agent.create_session().await?;
    let _closed = session.close().await?;
    let _shutdown = runtime.shutdown().await?;
    Ok(())
}

fn main() {}
