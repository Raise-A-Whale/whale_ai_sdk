//! Connection bootstrap; protocol versions are independent from crate releases.
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

pub const METHOD_INITIALIZE: &str = "protocol.initialize";
pub const PROTOCOL_VERSION: u32 = 1;
pub const SUPPORTED_PROTOCOL_VERSIONS: &[u32] = &[PROTOCOL_VERSION];
pub const PROTOCOL_CAPABILITIES: &[&str] = &[
    "runs.v1",
    "scoped_tools.v1",
    "tool_context.v1",
    "context_policy.v1",
    "session_close.v1",
    "provider_config.v1",
    "model_providers.v1",
    "sampling_options.v1",
];
pub const PROTOCOL_NOT_INITIALIZED: i64 = -32010;
pub const PROTOCOL_INCOMPATIBLE: i64 = -32011;
pub const PROTOCOL_ALREADY_INITIALIZED: i64 = -32012;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct PeerInfo {
    pub name: String,
    pub version: String,
}

impl PeerInfo {
    pub fn validate(&self) -> Result<(), String> {
        validate_name(&self.name, "Peer name")?;
        validate_name(&self.version, "Peer package version")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct InitializeParams {
    pub client: PeerInfo,
    pub protocol_versions: Vec<u32>,
    pub required_capabilities: Vec<String>,
}

impl InitializeParams {
    /// All features exposed by the current language SDKs are required at startup.
    pub fn sdk(name: impl Into<String>, version: impl Into<String>) -> Self {
        Self {
            client: PeerInfo {
                name: name.into(),
                version: version.into(),
            },
            protocol_versions: SUPPORTED_PROTOCOL_VERSIONS.to_vec(),
            required_capabilities: PROTOCOL_CAPABILITIES.iter().map(|s| (*s).into()).collect(),
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        self.client.validate()?;
        if self.protocol_versions.is_empty() {
            return Err("At least one supported protocol version is required".into());
        }
        let mut seen = HashSet::new();
        for version in &self.protocol_versions {
            if *version == 0 || !seen.insert(*version) {
                return Err("Protocol versions must be positive and unique".into());
            }
        }
        validate_capabilities(&self.required_capabilities)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct InitializeResult {
    pub server: PeerInfo,
    pub protocol_version: u32,
    pub capabilities: Vec<String>,
}

impl InitializeResult {
    /// Selects a supported version and reports actual daemon features.
    /// This function does not change connection state or call providers.
    pub fn negotiate(params: &InitializeParams, server: PeerInfo) -> Result<Self, String> {
        params.validate()?;
        server.validate()?;
        let protocol_version = params.protocol_versions.iter()
            .filter(|v| SUPPORTED_PROTOCOL_VERSIONS.contains(v))
            .max().copied()
            .ok_or_else(|| format!("No compatible protocol version; daemon supports {SUPPORTED_PROTOCOL_VERSIONS:?}"))?;
        let result = Self {
            server,
            protocol_version,
            capabilities: PROTOCOL_CAPABILITIES.iter().map(|s| (*s).into()).collect(),
        };
        result.validate_for(params)?;
        Ok(result)
    }

    /// Validate known fields while allowing additive bootstrap fields/features.
    pub fn validate_for(&self, params: &InitializeParams) -> Result<(), String> {
        params.validate()?;
        self.server.validate()?;
        if self.protocol_version == 0 || !params.protocol_versions.contains(&self.protocol_version)
        {
            return Err(format!(
                "Daemon selected unoffered protocol version {}",
                self.protocol_version
            ));
        }
        validate_capabilities(&self.capabilities)?;
        let missing: Vec<_> = params
            .required_capabilities
            .iter()
            .filter(|required| !self.capabilities.contains(required))
            .collect();
        if !missing.is_empty() {
            return Err(format!(
                "Daemon is missing required capabilities: {missing:?}"
            ));
        }
        Ok(())
    }
}

fn validate_name(value: &str, label: &str) -> Result<(), String> {
    if value.is_empty() || value.trim() != value {
        return Err(format!(
            "{label} must be nonempty without surrounding whitespace"
        ));
    }
    Ok(())
}

fn validate_capabilities(capabilities: &[String]) -> Result<(), String> {
    let mut seen = HashSet::new();
    for capability in capabilities {
        validate_name(capability, "Capability name")?;
        if !seen.insert(capability) {
            return Err(format!("Duplicate capability: {capability}"));
        }
    }
    Ok(())
}
