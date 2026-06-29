# common/exceptions — AWS

Typed exceptions so jobs fail with clear, catchable types. Step Functions can
branch on the error type.

`PlatformError` (base) → `ConfigError`, `SourceNotFoundError`, `SchemaDriftError`,
`DQError`, `WriteError`, `UpstreamNotReadyError`.

Keep class names identical to `databricks/src/common/exceptions/exceptions.py`.
