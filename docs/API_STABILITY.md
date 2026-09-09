# Rust API stability policy

This policy applies to the Rust source packages in the Whale `0.1.x` line. The
packages are still pre-1.0, but a patch release must remain source-compatible
with code that uses the documented public APIs. A future `0.x` minor release may
make a breaking change only when the release notes name the affected API and
give a migration path.

## Frozen compatibility surfaces

The following existing types are intentionally exhaustive. Code may construct
their public fields or match every variant, so adding a required field or enum
variant is a source-breaking change:

- `AgentDefinition`, `StartThreadParams`, and `RunSnapshot`;
- `AgentStreamEvent`, `RunEventPayload`, and the Session V1 snapshot/event
  family;
- `RuntimeMode`, `RuntimeSource`, `RuntimeOptions`, `RuntimeShutdown`,
  `RuntimeShutdownPhase`, `ShutdownDisposition`, `RuntimeError`, and
  `SdkError`.

Whale keeps these shapes unchanged throughout `0.1.x`. A new runtime transport,
runtime error family, or required option therefore needs either a new additive
entry point that preserves these types or a versioned breaking release. New
feature-specific SDK failures should use a dedicated `#[non_exhaustive]` error
type and wrap the existing `SdkError` where needed; they must not append a
variant to `SdkError` in a patch release.

Session V1 (`session.get`, `session.subscribe`, and `session.event`) remains a
separate frozen contract. Session management V2 does not add lifecycle or
metadata variants to V1 and uses its own cursor, snapshot, event, and typed
error families.

## Extensible surfaces

New protocol projections and feature-specific errors that are expected to grow
are marked `#[non_exhaustive]`. Consumers must use constructors where supplied,
read documented fields, and include a fallback arm when matching an extensible
enum. Examples include Session management V2 projections and
`SessionManagementError`, plus the Interaction request, pending projection,
snapshot, event, replay-reason, and `InteractionViewError` families. Interaction
wire params and cursor identity remain validated protocol contracts; stable
daemon response codes continue through the existing exhaustive `SdkError::Rpc`
variant instead of adding an `SdkError` variant in a patch release.

Extension traits such as `ModelProvider`, `HostContextPolicy`, `HostTool`,
`ToolPack`, and `SessionStore` are public implementation boundaries. Adding a
required trait method is breaking. Additive methods need a safe default body;
changes to cancellation, ownership, ordering, or persistence semantics require
the same review as a signature change.

## Protocol evolution

Optional wire features are gated by initialization capabilities. A client must
check the relevant capability before sending a feature method, and a daemon
must not send that feature's notifications before explicit opt-in or
subscription. New capability families use new method/type namespaces when an
existing reducer or JSON shape is frozen.

Handshake compatibility proves protocol support, not peer identity. External
Unix-domain-socket deployments are trusted-local-peer connections and must rely
on a host-controlled directory and socket permissions until an authenticated
transport is designed.

## Release checks

Before a Rust source-package release, Whale checks all six crate archives after
unpacking, compiles independent application and extension consumers, runs SDK
doctests and workspace tests, builds rustdoc with warnings denied, verifies the
declared Rust version, and records any unavailable advisory or API-diff tooling
as unchecked. No package is described as published until registry publication
has happened separately.
