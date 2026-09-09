//! Optional recovery transactions; mutation ownership outlives a caller's wait.
use crate::*;
use whale_protocol::recovery::*;

pub(crate) fn definitive_rejection(error: &SdkError) -> bool {
    matches!(
        error,
        SdkError::Rpc {
            code: -32601 | -32602 | RECOVERY_UNAVAILABLE | RECOVERY_REJECTED,
            ..
        } | SdkError::LimitExceeded(_)
    )
}
fn validate_snapshot(
    snapshot: RecoverySnapshot,
    key: &RecoveryKey,
) -> Result<RecoverySnapshot, SdkError> {
    if snapshot.recovery_id != key.recovery_id || snapshot.revision == 0 || snapshot.epoch == 0 {
        return Err(SdkError::Internal(
            "Recovery snapshot identity or revision mismatch".into(),
        ));
    }
    Ok(snapshot)
}
impl WhaleClient {
    pub(crate) async fn require_recovery(&self, key: &RecoveryKey) -> Result<(), SdkError> {
        key.validate().map_err(SdkError::InvalidConfiguration)?;
        let initialized = self.initialize().await?;
        if !initialized
            .capabilities
            .iter()
            .any(|c| c == CAPABILITY_SESSION_RECOVERY)
        {
            return Err(SdkError::ProtocolCompatibility(
                "Daemon does not advertise session_recovery.v1".into(),
            ));
        }
        Ok(())
    }

    /// Reads durable history and archived run snapshots; no work is resumed.
    pub async fn inspect_recovery(&self, key: &RecoveryKey) -> Result<RecoverySnapshot, SdkError> {
        self.require_recovery(key).await?;
        let snapshot = self
            .request(
                METHOD_RECOVERY_INSPECT,
                Some(InspectRecoveryParams { key: key.clone() }),
            )
            .await?;
        validate_snapshot(snapshot, key)
    }

    /// Permanently forgets a detached record at the inspected revision.
    pub async fn forget_session(
        &self,
        key: &RecoveryKey,
        expected_revision: u64,
    ) -> Result<bool, SdkError> {
        let params = ForgetRecoveryParams {
            key: key.clone(),
            expected_revision,
        };
        params.validate().map_err(SdkError::InvalidConfiguration)?;
        self.require_recovery(key).await?;
        let client = self.clone();
        let (tx, rx) = oneshot::channel();
        tokio::spawn(async move {
            let result = client
                .request::<_, ForgetRecoveryResult>(METHOD_RECOVERY_FORGET, Some(params.clone()))
                .await
                .and_then(|result| {
                    if result.recovery_id == params.key.recovery_id {
                        Ok(result.forgotten)
                    } else {
                        Err(SdkError::Internal(
                            "Recovery forget identity mismatch".into(),
                        ))
                    }
                });
            if result
                .as_ref()
                .err()
                .is_some_and(|error| !definitive_rejection(error))
            {
                client.close().await;
            }
            let _ = tx.send(result);
        });
        rx.await
            .map_err(|_| SdkError::ChannelClosed("Recovery forget task stopped".into()))?
    }
}
impl WhaleThread {
    pub fn recovery_key(&self) -> Option<&RecoveryKey> {
        self.recovery_key.as_ref()
    }

    /// Acknowledges exactly the unresolved execution IDs at this revision. It
    /// records an application decision; it does not claim those effects succeeded.
    pub async fn acknowledge_unknown(
        &self,
        expected_revision: u64,
        execution_ids: Vec<String>,
    ) -> Result<RecoverySnapshot, SdkError> {
        let key = self
            .recovery_key
            .clone()
            .ok_or_else(|| SdkError::InvalidConfiguration("Session is not persistent".into()))?;
        let params = AcknowledgeUnknownParams {
            key,
            expected_revision,
            execution_ids,
        };
        params.validate().map_err(SdkError::InvalidConfiguration)?;
        self.client.require_recovery(&params.key).await?;
        self.client
            .inner
            .state
            .ensure_session_open(&self.thread_id)?;
        let client = self.client.clone();
        let sid = self.thread_id.clone();
        let (tx, rx) = oneshot::channel();
        tokio::spawn(async move {
            let result = client
                .inner
                .state
                .request_for_session::<_, RecoverySnapshot>(
                    &client.inner.writer,
                    METHOD_RECOVERY_ACKNOWLEDGE,
                    Some(params.clone()),
                    &sid,
                )
                .await
                .and_then(|snapshot| validate_snapshot(snapshot, &params.key));
            if result.as_ref().err().is_some_and(|error| {
                !definitive_rejection(error) && !matches!(error, SdkError::SessionClosed(_))
            }) {
                client.close().await;
            }
            let _ = tx.send(result);
        });
        rx.await
            .map_err(|_| SdkError::ChannelClosed("Recovery acknowledge task stopped".into()))?
    }
}
