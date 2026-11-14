//! Goose-specific inference request preparation.

use crate::agents::extension_manager::{ExtensionLease, ExtensionManager};
use crate::agents::tool_deferral::{deferred_tool_names, ToolDeferralStrategy};
use crate::agents::PromptManager;
use crate::config::GooseMode;
use crate::session::Session;
use crate::tool_inspection::ToolInspectionManager;
use anyhow::Result;
use async_trait::async_trait;
use goose_agent::inference::{InferenceRequestPreparer, PreparedInferenceRequest};
use goose_agent::operation::{messages_since_kickoff, InferenceInput};
use goose_providers::conversation::message::Message;
use goose_providers::conversation::Conversation;
use std::collections::HashSet;
use std::sync::{Arc, Mutex as StdMutex};

pub struct GooseInferenceRequestPreparer<'a> {
    pub(crate) extension_manager: Arc<ExtensionManager>,
    pub(crate) extension_lease: Arc<StdMutex<Option<Arc<ExtensionLease>>>>,
    pub(crate) tool_inspection_manager: &'a ToolInspectionManager,
    pub(crate) context_limit: usize,
    pub(crate) deferral: ToolDeferralStrategy,
    pub(crate) always_load_tools: HashSet<String>,
}

#[async_trait]
impl InferenceRequestPreparer<Session> for GooseInferenceRequestPreparer<'_> {
    async fn prepare_session(&self, session: &Session) -> Result<Option<Session>> {
        let (session, lease) = self
            .extension_manager
            .current_session_snapshot(session)
            .await?;
        *self
            .extension_lease
            .lock()
            .expect("extension lease unavailable") = Some(Arc::new(lease));
        Ok(Some(session))
    }

    async fn prepare(
        &self,
        session: &Session,
        conversation: &Conversation,
        input: InferenceInput,
    ) -> Result<PreparedInferenceRequest> {
        #[cfg(feature = "code-mode")]
        let code_execution_mode = self
            .extension_lease
            .lock()
            .expect("extension lease unavailable")
            .as_ref()
            .is_some_and(|lease| {
                lease.is_enabled(crate::agents::platform_extensions::code_execution::EXTENSION_NAME)
            });
        #[cfg(not(feature = "code-mode"))]
        let code_execution_mode = false;

        let goose_mode = session.goose_mode;
        if goose_mode == GooseMode::SmartApprove {
            self.tool_inspection_manager
                .apply_tool_annotations(&input.tools);
        }
        let tools =
            crate::agents::reply_parts::prepare_inference_tools(input.tools, code_execution_mode);
        let system_prompt =
            PromptManager::new().build_system_prompt(session, input.prompt_parts, goose_mode)?;
        let turn = messages_since_kickoff(conversation)?;
        let turn_start = turn
            .first()
            .and_then(|message| chrono::DateTime::from_timestamp(message.created, 0))
            .map(|timestamp| timestamp.with_timezone(&chrono::Local))
            .unwrap_or_else(chrono::Local::now);
        let last = turn
            .iter()
            .rev()
            .find(|message| message.is_turn_context())
            .map(Message::as_concat_text);
        let context_limit = Some(self.context_limit);
        let additional_messages = crate::agents::moim::turn_context_event(
            &session.working_dir,
            context_limit,
            input.moim_parts,
            turn_start,
        )
        .filter(|event| Some(event.as_concat_text()) != last)
        .into_iter()
        .collect();
        Ok(PreparedInferenceRequest {
            system_prompt,
            tools,
            additional_messages,
            deferred_tools: self.deferred_tools().await,
        })
    }
}

impl GooseInferenceRequestPreparer<'_> {
    /// The schemas the provider, rather than goose, will let the model discover.
    /// Only a provider-native strategy defers anything: `lookup` withholds the
    /// schemas from the request by dropping them from the tool list.
    async fn deferred_tools(&self) -> Option<Vec<String>> {
        if !self.deferral.is_provider_native() {
            return None;
        }
        let lease = self
            .extension_lease
            .lock()
            .expect("extension lease unavailable")
            .clone()?;
        let names = deferred_tool_names(&lease, &self.always_load_tools).await;
        (!names.is_empty()).then_some(names)
    }
}
