//! How much of a tool's schema goose hands the model up front.
//!
//! Two strategies withhold MCP tool schemas, and the difference is who
//! publishes discovery: the prompt (goose's own `enable_tools` loop) or the
//! provider's native search tool.

use std::collections::HashSet;

use crate::agents::extension_manager::ExtensionLease;
use crate::agents::reply_parts::is_tool_visible_to_model;

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
) -> Vec<String> {
    lease
        .tools_where(|extension| !extension.is_platform())
        .await
        .into_iter()
        .filter(is_tool_visible_to_model)
        .filter(|tool| !always_load_tools.contains(tool.name.as_ref()))
        .map(|tool| tool.name.to_string())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::ToolDeferralStrategy;

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
    fn only_the_provider_native_strategy_publishes_its_own_discovery() {
        assert!(!ToolDeferralStrategy::Off.defers_tools());
        assert!(ToolDeferralStrategy::Lookup.defers_tools());
        assert!(ToolDeferralStrategy::Anthropic.defers_tools());
        assert!(!ToolDeferralStrategy::Lookup.is_provider_native());
        assert!(ToolDeferralStrategy::Anthropic.is_provider_native());
    }
}
