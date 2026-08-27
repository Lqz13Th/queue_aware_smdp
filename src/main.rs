use std::{env, path::PathBuf, sync::Arc};

use tokio::signal;
use tracing::{error, info};

use extrema_infra::prelude::*;

use queue_aware_smdp::arch::{
    execution_probe_module::{
        base::{BuiltProbe, RuntimeIdentity, build_probe, schedule_task},
        utils::{AppConfig, ProcessClock, build_commit, make_run_id},
    },
    schema::RunManifest,
    storage::{manifest_streams, start_storage},
};

#[tokio::main]
async fn main() -> InfraResult<()> {
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .map_err(|_| InfraError::Msg("failed to install rustls crypto provider".into()))?;
    tracing_subscriber::fmt::init();

    let config_path = config_path()?;
    let config = Arc::new(AppConfig::load(&config_path)?);
    let clock = ProcessClock::new()?;
    let run_id = make_run_id(&config.collector.host_id, clock.process_start_wall_ns);
    let build_commit = build_commit();
    let identity = RuntimeIdentity {
        run_id: run_id.clone(),
        host_id: config.collector.host_id.clone(),
        build_commit: build_commit.clone(),
        clock,
    };
    let manifest = RunManifest {
        schema_version: queue_aware_smdp::arch::storage::schema_version(),
        run_id: run_id.clone(),
        host_id: config.collector.host_id.clone(),
        process_id: std::process::id(),
        build_commit,
        process_start_wall_ns: identity.clock.process_start_wall_ns,
        config_path: config_path.display().to_string(),
        probe_enabled: config.probe.enabled,
        streams: manifest_streams(),
    };
    let (storage, storage_task) = start_storage(&config.collector, &run_id, &manifest).await?;
    let BuiltProbe {
        strategy,
        ws_tasks,
        background_tasks,
        mut control,
    } = match build_probe(Arc::clone(&config), identity, storage.clone()).await {
        Ok(built) => built,
        Err(err) => {
            let _ = storage.shutdown().await;
            let _ = storage_task.await;
            return Err(err);
        }
    };

    let env = EnvBuilder::new()
        .with_tasks(ws_tasks)
        .with_task(schedule_task(config.collector.schedule_interval_ms))
        .with_strategy_module(strategy)
        .build()?;
    info!(%run_id, probe_enabled = config.probe.enabled, "collector started");
    let runtime = tokio::spawn(async move { env.execute().await });

    shutdown_signal().await?;
    info!("shutdown requested; waiting for order and inventory reconciliation");
    control
        .shutdown
        .send(true)
        .map_err(|_| InfraError::Msg("probe event loop stopped before shutdown".into()))?;
    control
        .shutdown_complete
        .recv()
        .await
        .ok_or_else(|| InfraError::Msg("probe shutdown acknowledgement channel closed".into()))?;

    runtime.abort();
    let _ = runtime.await;
    let _ = control.stop_workers.send(true);
    for task in background_tasks {
        match task.await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => error!(?err, "background collector failed during shutdown"),
            Err(err) => error!(?err, "background collector task join failed"),
        }
    }
    storage.shutdown().await?;
    storage_task
        .await
        .map_err(|err| InfraError::Msg(format!("storage task join failed: {err}")))??;
    info!(%run_id, "collector stopped cleanly");
    Ok(())
}

fn config_path() -> InfraResult<PathBuf> {
    let mut args = env::args_os().skip(1);
    match args.next() {
        None => Ok(PathBuf::from("strategy_config.toml")),
        Some(flag) if flag == "--config" => {
            let path = args
                .next()
                .ok_or_else(|| InfraError::Msg("--config requires a path".into()))?;
            if args.next().is_some() {
                return Err(InfraError::Msg("unexpected command-line arguments".into()));
            }
            Ok(path.into())
        }
        Some(other) => Err(InfraError::Msg(format!(
            "unknown argument {}; expected --config <path>",
            other.to_string_lossy()
        ))),
    }
}

async fn shutdown_signal() -> InfraResult<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            signal::unix::signal(signal::unix::SignalKind::terminate()).map_err(InfraError::Io)?;
        tokio::select! {
            result = signal::ctrl_c() => result.map_err(InfraError::Io),
            _ = terminate.recv() => Ok(()),
        }
    }
    #[cfg(not(unix))]
    {
        signal::ctrl_c().await.map_err(InfraError::Io)
    }
}
