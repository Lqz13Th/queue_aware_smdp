# Queue-Aware SMDP

`queue_aware_smdp` is the live calibration collector for the future XYZ100
Queue-Aware Semi-Markov Decision Process market-making strategy. It connects
to the public and private feeds, records immutable raw telemetry, and can run a
controlled one-order Hyperliquid probe policy.

## Responsibility

The collector owns two operations:

1. Continuously record the public market, private account, and local system
   events needed for causal execution analysis.
2. Run one controlled minimum-size Hyperliquid probe order at a time and record
   its complete decision, action, acknowledgement, order, fill, cancellation,
   markout, flattening, and reconciliation lifecycle.

It will write immutable raw telemetry only. Normalization, queue calibration,
order exposures, SMDP transitions, feature generation, notebooks, simulation,
and model training remain outside this repository.

## Architecture

```text
src/arch/execution_probe_module/
    base.rs       module struct, construction, lifecycle and probe loops
    core.rs       extrema_infra trait implementations only
    state.rs      market, account, order and queue-belief data structures
    utils.rs      configuration, identifiers, eligibility and safety checks

src/arch/action_gateway.rs
    place, cancel, and reduce-only IOC transport boundary

src/arch/storage.rs
    asynchronous append-only storage, rotation, recovery and backpressure

src/arch/schema.rs
    raw event, decision, action, account and system record contracts
```

`action_gateway` is a transport and safety boundary. It must not become a
second economic controller. Probe selection and all explicit `KEEP`, `PLACE`,
`CANCEL`, `REPLACE`, and flatten decisions belong to the probe module and must
be recorded.

## Input streams

```text
Hyperliquid XYZ100: BBO, fast L2, default L2, trades, activeAssetCtx
Binance QQQ:        bookTicker, depth@100ms, aggTrade
OKX QQQ:            bbo-tbt, books, trades-all
Hyperliquid private: orderUpdates, userFills, clearinghouseState
System:             connection state, reconnects, chrony and local clocks
```

The public and private streams must use the same process clock on the Tokyo
execution host.

## Probe boundary

```text
size:                 current minimum valid XYZ100 quantity
side:                 bid or ask
price:                BBO or one tick behind BBO
planned dwell:        30 / 60 / 120 / 300 seconds
time in force:        ALO
priority fee:         0
maximum active probe: 1
```

Increasing size is not a remedy for sparse fills. The experiment varies side,
price level, dwell, and session while keeping the order economically small.

## Raw storage contract

```text
runs/<run_id>/
├── manifest.json
└── raw/
    ├── public_market.jsonl.zst
    ├── private_ws.jsonl.zst
    ├── decisions.jsonl.zst
    ├── actions.jsonl.zst
    ├── account_snapshots.jsonl.zst
    └── system.jsonl.zst
```

Files rotate hourly. The active file ends in `.jsonl.partial`; clean shutdown
or rotation produces `.jsonl.zst` plus a `.sha256` sidecar. Raw files are
append-only and immutable after finalization. Credentials, private keys, and
environment variables are never persisted. Signed outbound action payloads
are retained because they are part of the causal execution record.

`WsOtherMessage.timestamp` is the infra socket-receive wall timestamp. Infra
does not currently expose its socket-read monotonic timestamp, so
`received_monotonic_ns` is sampled at the beginning of the strategy callback.
It preserves same-process causal order but must not be presented as exact
socket-read latency.

## Run

Copy the example configuration to `strategy_config.toml`, then provide the
Hyperliquid owner and API-agent credentials through the environment expected
by `extrema_infra`.

```bash
cargo run --release -- --config strategy_config.toml
```

With `probe.enabled = false`, all configured public/private streams and REST
snapshots are collected but no order action is sent. Live probing has three
independent gates:

```text
probe.enabled = true
probe.i_understand_live_orders = true
QUEUE_AWARE_SMDP_ALLOW_LIVE=1
```

Every live probe uses one ALO order, priority fee zero, current minimum valid
size, and at most one active order. A clean SIGINT/SIGTERM first drives the
probe through cancel/reconciliation and reduce-only flattening when needed,
then finalizes storage.

The Hyperliquid credentials are required even in collection-only mode because
the private order, fill, clearinghouse, fee, metadata, and rate-limit state are
part of the dataset.

## Boundary

This repository does not normalize data, create features, infer exact FIFO
position, generate counterfactual samples, train models, or implement the
event-driven SMDP controller. Those stages consume the immutable run output
offline.
