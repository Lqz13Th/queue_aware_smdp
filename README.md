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
size:                  current minimum valid XYZ100 quantity
side:                  bid or ask
price level from BBO:  0 / 1 / 2 / 3 / 5 / 8 ticks
planned open dwell:    30 / 60 / 120 / 300 seconds
candidate cells:       2 sides x 6 levels x 4 dwell = 48
time in force:         ALO
priority fee:          0
maximum active probe:  1
```

Increasing size is not a remedy for sparse fills. The experiment varies side,
price level, dwell, and session while keeping the order economically small.
The price level is measured from the same-side Hyperliquid BBO. Planned dwell
starts at the first private `orderUpdates=open`, not at PLACE socket write.
Each cell is selected uniformly with replacement and its probability is stored
in the decision record.

`price_level_ticks` is the placement-time experiment cell, not the order's
permanent distance from the market. The order price remains fixed while the
book moves. Decision, fill, and terminal records therefore also contain the
current BBO distance, fair-value distance in ticks and basis points, spread,
open age, and remaining dwell. A negative current distance means that the
resting quote has become stale relative to the latest market, not that its
original cell was mislabeled.

## Probe lifecycle

```text
wait for eligibility
    -> sample one of 48 cells
    -> send one ALO PLACE over the Hyperliquid action websocket
    -> correlate post response, private orderUpdates, and REST state
    -> start dwell on the first private orderUpdates=open
    -> record KEEP decisions every keep_interval_ms while exposure is valid
    -> finish through cancel or fill
    -> collect 100 / 500 / 1000 / 5000 ms markouts for every fill
    -> flatten non-zero inventory with a reduce-only IOC
    -> reconcile zero open orders and zero inventory
    -> finalize the probe and enter cooldown
```

`KEEP` is decision telemetry; it does not send an exchange action. An unfilled
order is cancelled after its planned open dwell. A partial fill causes the
remaining maker quantity to be cancelled before markout and flattening. The
collector can end exposure earlier when market data is stale, the quote moves
outside the fair-value guard, storage backpressure becomes unsafe, or a hard
inventory limit is reached. Post, cancel, and flatten timeouts enter explicit
recovery and REST reconciliation rather than silently starting another probe.

The queue belief is an interval estimate derived from aggregate book and trade
events. It records visible size, order count, adds, removals, same-price taker
flow, and lower/upper queue-ahead bounds; it is not represented as an exact
FIFO position.

Each fill links back to its probe, decision, action, CLOID, and exchange order
ID. Fill telemetry retains the exchange fee and derives the realized fee rate
and fee in ticks alongside the account's maker/taker fee schedule. Flatten
fills are linked separately so passive execution economics and forced exit
costs can be analyzed independently.

## Raw storage contract

```text
runs/<run_id>/
├── manifest.json
└── raw/
    ├── public_market.part-<UTC-hour>.jsonl.zst
    ├── private_ws.part-<UTC-hour>.jsonl.zst
    ├── decisions.part-<UTC-hour>.jsonl.zst
    ├── actions.part-<UTC-hour>.jsonl.zst
    ├── account_snapshots.part-<UTC-hour>.jsonl.zst
    └── system.part-<UTC-hour>.jsonl.zst
```

Files rotate hourly. The active file ends in `.jsonl.partial`; clean shutdown
or rotation produces `.jsonl.zst` plus a `.sha256` sidecar. Raw files are
append-only and immutable after finalization. Credentials, private keys, and
environment variables are never persisted. Signed outbound action payloads
are retained because they are part of the causal execution record.

The recorded candidate set, selected action, selection probability, policy ID,
seed, and build commit are the authoritative behavior-policy trace. A seed by
itself is not a stable replay contract across dependency or RNG implementation
upgrades.

`WsOtherMessage.timestamp` is the infra socket-receive wall timestamp. Infra
does not currently expose its socket-read monotonic timestamp, so
`received_monotonic_ns` is sampled at the beginning of the strategy callback.
It preserves same-process causal order but must not be presented as exact
socket-read latency.

## Install

Install the published binary from crates.io:

```bash
cargo install queue_aware_smdp
```

## Run

Copy the example configuration to `strategy_config.toml`, then provide the
Hyperliquid owner and API-agent credentials through the environment expected
by `extrema_infra`.

```bash
cargo run --release -- --config strategy_config.toml
```

An installed binary can be started with the same configuration:

```bash
queue_aware_smdp --config strategy_config.toml
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
