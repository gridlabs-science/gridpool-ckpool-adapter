# GridPool CKPool Adapter

Status: early integration prototype. Do not use for unattended production mining.

Project-wide architecture and protocol context live in the
[GridPool handbook](https://github.com/gridlabs-science/gridpool-handbook). The
[GridPool reference node](https://github.com/gridlabs-science/boot-protocol)
remains the consensus validator.

This daemon keeps GridPool-specific HTTP, event-stream, retry, and fee-schedule
logic outside CKPool. A locally patched CKPool process exchanges versioned
length-prefixed JSON messages with it over a Unix socket.

## Responsibilities

- Validate and cache the current GridPool payout plan.
- Notify CKPool when the parent or active payout snapshot changes.
- Make deterministic slot-0 fee-window decisions.
- Durably queue complete GridPool share proofs.
- Batch local vardiff telemetry without treating it as consensus work.

The adapter never determines consensus validity. GridPool validates every full
proof, and CKPool submits possible blocks directly to the local Bitcoin node
before adapter notification.

## Build

```bash
cargo build --release
cp config/example.toml config/local.toml
```

The GridPool node and adapter must share the token referenced by
`adapter_token_file`. Keep `config/local.toml`, the token, fee secret, and queue
database outside version control.

Valid SSE heartbeats refresh the cached plan age. The IPC interface refuses
plans and fee decisions after `maximum_plan_age_seconds`, preventing a
disconnected sidecar from silently serving an indefinitely stale payout plan.

## IPC

Each request and response is UTF-8 JSON prefixed by a four-byte big-endian
length. Schema version 1 supports `get_plan`, `fee_decision`, `submit_proof`,
`record_share`, `submit_telemetry`, and `health`. `record_share` is aggregated
for ten seconds before HTTP delivery, so ordinary vardiff traffic never blocks
CKPool's share path. The default maximum frame is 256 KiB.

## Fee Semantics

For a configured 150 basis-point Atlas fee, approximately 1.5% of ten-second
work buckets use the Atlas operator address in slot 0. These remain valid
GridPool templates. Their proofs and any block-finder reward are attributed to
the operator, so the same 1.5% work fraction supplies an approximately 1.5%
long-run fee without taking an entire non-GridPool block.

The telemetry identity remains the connected miner during fee windows while
`feeWorkDifficulty` records the redirected fraction. Full proof attribution is
always derived from the actual operator slot-0 output.

## Service Installation

Build with `cargo build --release`, install the binary as
`/usr/local/bin/gridpool-ckpool-adapter`, and customize the paths in
`deploy/gridpool-ckpool-adapter.service`. The service deliberately starts
before CKPool and owns the local Unix socket. CKPool must fail closed when this
service has no current parent-matching plan. Set `ckpool_notify_socket` to
CKPool's `stratifier` socket to trigger an immediate workbase refresh whenever
the adapter installs a new payout plan; CKPool's normal update interval remains
the fallback.

## License

Licensed under the [MIT License](LICENSE).
