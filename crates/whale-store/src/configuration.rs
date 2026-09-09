use crate::{Result, StoreError};
use serde_json::{json, Map, Value};
use whale_protocol::{
    recovery::SessionRunDefaults, session_management::validate_session_metadata_replacement,
};

/// Checked durable configuration with mutable metadata outside attachment identity.
///
/// The public name is retained for source compatibility with the Stage 2.2
/// parser. Parsed values are normalized to the current V3 wire shape.
#[derive(Debug, Clone, PartialEq)]
pub struct PersistedSessionConfigurationV2 {
    session: Map<String, Value>,
    run_defaults: Value,
    metadata: Map<String, Value>,
    interactions_enabled: bool,
}

impl PersistedSessionConfigurationV2 {
    pub fn parse_and_migrate(value: Value) -> Result<(Self, bool)> {
        let Value::Object(mut root) = value else {
            return Err(StoreError::Invalid(
                "Persisted Session configuration must be an object".into(),
            ));
        };
        let version = root
            .remove("version")
            .and_then(|value| value.as_u64())
            .ok_or_else(|| {
                StoreError::Invalid(
                    "Persisted Session configuration requires an integer version".into(),
                )
            })?;
        match version {
            1 => Self::migrate_v1(root).map(|configuration| (configuration, true)),
            2 => Self::parse_v2(root).map(|configuration| (configuration, true)),
            3 => Self::parse_v3(root).map(|configuration| (configuration, false)),
            other => Err(StoreError::Invalid(format!(
                "Unsupported persisted Session configuration version {other}"
            ))),
        }
    }

    pub fn attachment_identity(&self) -> (&Map<String, Value>, &Value) {
        (&self.session, &self.run_defaults)
    }

    pub fn metadata(&self) -> &Map<String, Value> {
        &self.metadata
    }

    pub fn interactions_enabled(&self) -> bool {
        self.interactions_enabled
    }

    pub fn into_value(self) -> Value {
        json!({
            "version": 3,
            "session": self.session,
            "run_defaults": self.run_defaults,
            "metadata": self.metadata,
            "runtime_features": {
                "interactions_enabled": self.interactions_enabled,
            },
        })
    }

    pub(crate) fn replace_metadata(&mut self, metadata: Map<String, Value>) -> Result<()> {
        validate_metadata_replacement(&metadata)?;
        self.metadata = metadata;
        Ok(())
    }

    fn migrate_v1(mut root: Map<String, Value>) -> Result<Self> {
        let (mut session, run_defaults) = match root.remove("session") {
            Some(Value::Object(session)) => {
                let run_defaults = root.remove("run_defaults").unwrap_or_else(|| json!({}));
                if !root.is_empty() {
                    return Err(StoreError::Invalid(
                        "Persisted Session configuration V1 has unknown top-level fields".into(),
                    ));
                }
                (session, run_defaults)
            }
            Some(_) => {
                return Err(StoreError::Invalid(
                    "Persisted Session configuration session must be an object".into(),
                ))
            }
            None => {
                // Early StoreRuntime callers supplied a compact V1 object directly.
                // Preserve that source-compatible shape by treating its remaining
                // fields as attachment identity during the one-time migration.
                let run_defaults = root.remove("run_defaults").unwrap_or_else(|| json!({}));
                (root, run_defaults)
            }
        };
        let metadata = take_metadata(&mut session, true)?;
        validate_run_defaults(&run_defaults)?;
        validate_session_identity(&session)?;
        Ok(Self {
            session,
            run_defaults,
            metadata,
            interactions_enabled: false,
        })
    }

    fn parse_v2(mut root: Map<String, Value>) -> Result<Self> {
        let session = match root.remove("session") {
            Some(Value::Object(session)) => session,
            _ => {
                return Err(StoreError::Invalid(
                    "Persisted Session configuration V2 requires an object session".into(),
                ))
            }
        };
        let run_defaults = root.remove("run_defaults").ok_or_else(|| {
            StoreError::Invalid("Persisted Session configuration V2 requires run_defaults".into())
        })?;
        let metadata = match root.remove("metadata") {
            Some(Value::Object(metadata)) => metadata,
            _ => {
                return Err(StoreError::Invalid(
                    "Persisted Session configuration V2 requires object metadata".into(),
                ))
            }
        };
        if !root.is_empty() {
            return Err(StoreError::Invalid(
                "Persisted Session configuration V2 has unknown top-level fields".into(),
            ));
        }
        validate_run_defaults(&run_defaults)?;
        validate_session_identity(&session)?;
        if session.contains_key("metadata") {
            return Err(StoreError::Invalid(
                "Persisted Session configuration V2 metadata must be top-level".into(),
            ));
        }
        Ok(Self {
            session,
            run_defaults,
            metadata,
            interactions_enabled: false,
        })
    }

    fn parse_v3(mut root: Map<String, Value>) -> Result<Self> {
        let session = match root.remove("session") {
            Some(Value::Object(session)) => session,
            _ => {
                return Err(StoreError::Invalid(
                    "Persisted Session configuration V3 requires an object session".into(),
                ))
            }
        };
        let run_defaults = root.remove("run_defaults").ok_or_else(|| {
            StoreError::Invalid("Persisted Session configuration V3 requires run_defaults".into())
        })?;
        let metadata = match root.remove("metadata") {
            Some(Value::Object(metadata)) => metadata,
            _ => {
                return Err(StoreError::Invalid(
                    "Persisted Session configuration V3 requires object metadata".into(),
                ))
            }
        };
        let interactions_enabled = match root.remove("runtime_features") {
            Some(Value::Object(mut features)) => {
                let enabled = features
                    .remove("interactions_enabled")
                    .and_then(|value| value.as_bool())
                    .ok_or_else(|| {
                        StoreError::Invalid(
                            "Persisted Session configuration V3 requires boolean runtime_features.interactions_enabled"
                                .into(),
                        )
                    })?;
                if !features.is_empty() {
                    return Err(StoreError::Invalid(
                        "Persisted Session configuration V3 has unknown runtime_features".into(),
                    ));
                }
                enabled
            }
            _ => {
                return Err(StoreError::Invalid(
                    "Persisted Session configuration V3 requires object runtime_features".into(),
                ))
            }
        };
        if !root.is_empty() {
            return Err(StoreError::Invalid(
                "Persisted Session configuration V3 has unknown top-level fields".into(),
            ));
        }
        validate_run_defaults(&run_defaults)?;
        validate_session_identity(&session)?;
        if session.contains_key("metadata") {
            return Err(StoreError::Invalid(
                "Persisted Session configuration V3 metadata must be top-level".into(),
            ));
        }
        Ok(Self {
            session,
            run_defaults,
            metadata,
            interactions_enabled,
        })
    }
}

fn take_metadata(
    session: &mut Map<String, Value>,
    default_empty: bool,
) -> Result<Map<String, Value>> {
    match session.remove("metadata") {
        Some(Value::Object(metadata)) => Ok(metadata),
        Some(_) => Err(StoreError::Invalid(
            "Persisted Session metadata must be an object".into(),
        )),
        None if default_empty => Ok(Map::new()),
        None => Err(StoreError::Invalid(
            "Persisted Session metadata is missing".into(),
        )),
    }
}

fn validate_run_defaults(value: &Value) -> Result<()> {
    let defaults: SessionRunDefaults = serde_json::from_value(value.clone()).map_err(|error| {
        StoreError::Invalid(format!("Invalid persisted Session run_defaults: {error}"))
    })?;
    defaults.validate().map_err(StoreError::Invalid)
}

fn validate_session_identity(session: &Map<String, Value>) -> Result<()> {
    if session.contains_key("session_id") {
        return Err(StoreError::Invalid(
            "Persisted Session attachment identity cannot contain session_id".into(),
        ));
    }
    if let Some(tools) = session.get("tools") {
        let tools = tools.as_array().ok_or_else(|| {
            StoreError::Invalid("Persisted Session tools must be an array".into())
        })?;
        for tool in tools {
            let tool = tool.as_object().ok_or_else(|| {
                StoreError::Invalid("Persisted Session tools must contain objects".into())
            })?;
            if tool.contains_key("binding_id") {
                return Err(StoreError::Invalid(
                    "Persisted Session tools cannot contain host binding_id".into(),
                ));
            }
        }
    }
    Ok(())
}

fn validate_metadata_replacement(metadata: &Map<String, Value>) -> Result<()> {
    validate_session_metadata_replacement(metadata).map_err(StoreError::Invalid)
}
