//! How much of a tool's schema goose hands the model up front.
//!
//! Two strategies withhold MCP tool schemas, and the difference is who
//! publishes discovery: the prompt (goose's own `enable_tools` loop) or the
//! provider's native search tool.

use std::collections::HashSet;

use crate::agents::extension_manager::ExtensionLease;
use crate::agents::reply_parts::is_tool_visible_to_model;
use crate::conversation::message::MessageContent;
use crate::conversation::Conversation;

/// The `GOOSE_TOOL_DEFERRAL` setting.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ToolDeferralStrategy {
    /// Every schema is sent with the request. The default.
    #[default]
    Off,
    /// Schemas are withheld and the model asks for them with `enable_tools`.
    /// Portable across providers, because goose does the publishing.
    Lookup,
    /// Schemas are withheld and the provider's own tool search finds them.
    /// Provider-native: only the Anthropic format implements it.
    Anthropic,
}

impl ToolDeferralStrategy {
    /// `lookup` and `anthropic` are matched exactly; anything else, including
    /// an unset value, is `off`. `lookup` keeps the semantics it had when the
    /// setting was a boolean.
    pub(crate) fn from_setting(value: Option<&str>) -> Self {
        match value {
            Some("lookup") => Self::Lookup,
            Some("anthropic") => Self::Anthropic,
            _ => Self::Off,
        }
    }

    /// Whether MCP tool schemas are kept out of the request at all.
    pub(crate) fn defers_tools(self) -> bool {
        !matches!(self, Self::Off)
    }

    /// Whether the provider, rather than goose's prompt, tells the model that
    /// tools can be discovered — so goose publishes nothing itself.
    pub(crate) fn is_provider_native(self) -> bool {
        matches!(self, Self::Anthropic)
    }
}

/// The MCP tool names a deferring strategy withholds from the request: the
/// non-platform, model-visible tools not pinned eager by
/// `GOOSE_TOOL_DEFERRAL_ALWAYS_LOAD`. Both strategies defer the same names;
/// they differ only in what tells the model the names exist.
pub(crate) async fn deferred_tool_names(
    lease: &ExtensionLease,
    always_load_tools: &HashSet<String>,
    keep_loaded: &HashSet<String>,
) -> Vec<String> {
    lease
        .tools_where(|extension| !extension.is_platform())
        .await
        .into_iter()
        .filter(is_tool_visible_to_model)
        .filter(|tool| !always_load_tools.contains(tool.name.as_ref()))
        .filter(|tool| !keep_loaded.contains(tool.name.as_ref()))
        .map(|tool| tool.name.to_string())
        .collect()
}

/// The tools the conversation has called whose discovery is no longer in it.
///
/// The search that returned a tool's definition is a block in the transcript, and
/// compaction rewrites the transcript — the definition goes with it while the calls
/// to that tool survive in the summary or the tail. A tool the model has used is one
/// it knows, so those are kept eager instead of being withheld again.
pub(crate) fn called_without_discovery(conversation: Option<&Conversation>) -> HashSet<String> {
    let Some(conversation) = conversation else {
        return HashSet::new();
    };
    let mut called = HashSet::new();
    let mut discovered = HashSet::new();
    for message in conversation.messages() {
        for content in &message.content {
            if let Some(request) = content.as_tool_request() {
                if let Ok(call) = &request.tool_call {
                    called.insert(call.name.to_string());
                }
            }
            if let MessageContent::ProviderNative(native) = content {
                if native.block_type == "tool_search_tool_result" {
                    let references = native
                        .raw
                        .pointer("/content/tool_references")
                        .and_then(|value| value.as_array());
                    for reference in references.into_iter().flatten() {
                        if let Some(name) = reference.get("tool_name").and_then(|v| v.as_str()) {
                            discovered.insert(name.to_string());
                        }
                    }
                }
            }
        }
    }
    called.retain(|name| !discovered.contains(name));
    called
}

#[cfg(test)]
mod tests {
    use super::{called_without_discovery, Conversation, ToolDeferralStrategy};

    #[test]
    fn lookup_and_anthropic_are_recognised() {
        assert_eq!(
            ToolDeferralStrategy::from_setting(Some("lookup")),
            ToolDeferralStrategy::Lookup
        );
        assert_eq!(
            ToolDeferralStrategy::from_setting(Some("anthropic")),
            ToolDeferralStrategy::Anthropic
        );
    }

    #[test]
    fn everything_else_is_off() {
        for value in [
            None,
            Some(""),
            Some("off"),
            Some("true"),
            Some("LOOKUP"),
            Some(" lookup"),
        ] {
            assert_eq!(
                ToolDeferralStrategy::from_setting(value),
                ToolDeferralStrategy::Off,
                "{value:?} must not enable deferral"
            );
        }
    }

    #[test]
    fn a_tool_the_conversation_called_survives_losing_its_discovery_block() {
        use crate::conversation::message::Message;
        use rmcp::model::CallToolRequestParams;

        let searched = Conversation::new_unvalidated([
            Message::assistant()
                .with_provider_native(
                    "tool_search_tool_result",
                    serde_json::json!({
                        "type": "tool_search_tool_result",
                        "content": {
                            "type": "tool_search_tool_search_result",
                            "tool_references": [{"type": "tool_reference", "tool_name": "comfyui__comfy_models"}],
                        }
                    }),
                )
                .with_tool_request(
                    "call-1",
                    Ok(CallToolRequestParams::new("comfyui__comfy_status")),
                )
                .with_tool_request(
                    "call-2",
                    Ok(CallToolRequestParams::new("comfyui__comfy_models")),
                ),
        ]);

        // `comfy_models` is still discovered by the block above; `comfy_status` was called without one
        let keep = called_without_discovery(Some(&searched));
        assert!(keep.contains("comfyui__comfy_status"));
        assert!(!keep.contains("comfyui__comfy_models"));

        // compaction rewrites history: the block goes, the calls can stay — then both stay loaded
        let compacted = Conversation::new_unvalidated([Message::assistant().with_tool_request(
            "call-1",
            Ok(CallToolRequestParams::new("comfyui__comfy_status")),
        )]);
        assert_eq!(
            called_without_discovery(Some(&compacted)),
            ["comfyui__comfy_status".to_string()].into_iter().collect()
        );
        assert!(called_without_discovery(None).is_empty());
    }

    #[test]
    fn only_the_provider_native_strategy_publishes_its_own_discovery() {
        assert!(!ToolDeferralStrategy::Off.defers_tools());
        assert!(ToolDeferralStrategy::Lookup.defers_tools());
        assert!(ToolDeferralStrategy::Anthropic.defers_tools());
        assert!(!ToolDeferralStrategy::Lookup.is_provider_native());
        assert!(ToolDeferralStrategy::Anthropic.is_provider_native());
    }
}
