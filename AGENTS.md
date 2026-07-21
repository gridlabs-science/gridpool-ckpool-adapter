# GridPool CKPool Adapter Agent Guide

This daemon is the reliability boundary between CKPool and a local GridPool
node. It does not decide consensus validity.

- Keep IPC versioned, bounded, local, and fail-closed.
- Validate every work-plan field before publishing it to CKPool.
- Persist complete proofs before acknowledging queue acceptance.
- Never store API tokens, fee secrets, or credentials in git.
- Fee scheduling changes slot-0 attribution only; the GridPool suffix remains
  exact and consensus-valid.

Validate with `cargo fmt --check` and `cargo test`.

