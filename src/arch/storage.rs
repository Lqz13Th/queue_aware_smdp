use std::{
    collections::BTreeMap,
    fs::{self as std_fs, File as StdFile, OpenOptions as StdOpenOptions},
    io::{self, BufReader, Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use chrono::{Datelike, Timelike, Utc};
use serde::Serialize;
use sha2::{Digest, Sha256};
use tokio::{
    fs::{self, File, OpenOptions},
    io::{AsyncWriteExt, BufWriter},
    sync::mpsc,
    task::JoinHandle,
    time::{MissedTickBehavior, interval},
};

use extrema_infra::errors::{InfraError, InfraResult};

use super::{
    execution_probe_module::utils::CollectorConfig,
    schema::{RunManifest, SCHEMA_VERSION},
};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum StorageStream {
    PublicMarket,
    PrivateWs,
    Decisions,
    Actions,
    AccountSnapshots,
    System,
}

impl StorageStream {
    pub const ALL: [Self; 6] = [
        Self::PublicMarket,
        Self::PrivateWs,
        Self::Decisions,
        Self::Actions,
        Self::AccountSnapshots,
        Self::System,
    ];

    pub fn file_stem(self) -> &'static str {
        match self {
            Self::PublicMarket => "public_market",
            Self::PrivateWs => "private_ws",
            Self::Decisions => "decisions",
            Self::Actions => "actions",
            Self::AccountSnapshots => "account_snapshots",
            Self::System => "system",
        }
    }
}

enum StorageMessage {
    Record {
        stream: StorageStream,
        line: Vec<u8>,
    },
    Shutdown,
}

#[derive(Clone)]
pub struct StorageHandle {
    sender: mpsc::Sender<StorageMessage>,
}

impl StorageHandle {
    pub async fn record<T: Serialize>(&self, stream: StorageStream, value: &T) -> InfraResult<()> {
        let mut line = serde_json::to_vec(value)
            .map_err(|err| InfraError::Msg(format!("serialize telemetry: {err}")))?;
        line.push(b'\n');
        self.sender
            .send(StorageMessage::Record { stream, line })
            .await
            .map_err(|_| InfraError::Msg("telemetry writer stopped".into()))
    }

    pub fn remaining_capacity(&self) -> usize {
        self.sender.capacity()
    }

    pub async fn shutdown(&self) -> InfraResult<()> {
        self.sender
            .send(StorageMessage::Shutdown)
            .await
            .map_err(|_| InfraError::Msg("telemetry writer already stopped".into()))
    }
}

struct ActiveSegment {
    hour_key: String,
    path: PathBuf,
    writer: BufWriter<File>,
}

pub async fn start_storage(
    config: &CollectorConfig,
    run_id: &str,
    manifest: &RunManifest,
) -> InfraResult<(StorageHandle, JoinHandle<InfraResult<()>>)> {
    let run_root = config.data_root.join("runs").join(run_id);
    let raw_root = run_root.join("raw");
    fs::create_dir_all(&raw_root)
        .await
        .map_err(InfraError::Io)?;
    recover_abandoned_segments(&config.data_root, config.zstd_level)?;
    write_manifest(&run_root, manifest).await?;

    let (sender, receiver) = mpsc::channel(config.writer_capacity);
    let handle = StorageHandle { sender };
    let task_config = config.clone();
    let task = tokio::spawn(async move { writer_actor(task_config, raw_root, receiver).await });
    Ok((handle, task))
}

async fn write_manifest(run_root: &Path, manifest: &RunManifest) -> InfraResult<()> {
    let bytes = serde_json::to_vec_pretty(manifest)
        .map_err(|err| InfraError::Msg(format!("serialize manifest: {err}")))?;
    let temporary = run_root.join("manifest.json.partial");
    let final_path = run_root.join("manifest.json");
    fs::write(&temporary, bytes).await.map_err(InfraError::Io)?;
    fs::rename(temporary, final_path)
        .await
        .map_err(InfraError::Io)
}

async fn writer_actor(
    config: CollectorConfig,
    raw_root: PathBuf,
    mut receiver: mpsc::Receiver<StorageMessage>,
) -> InfraResult<()> {
    let mut segments = open_all_segments(&raw_root).await?;
    let mut flush_tick = interval(Duration::from_millis(config.flush_interval_ms));
    flush_tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    flush_tick.tick().await;
    let mut last_sync = Instant::now();

    loop {
        tokio::select! {
            message = receiver.recv() => {
                match message {
                    Some(StorageMessage::Record { stream, line }) => {
                        rotate_if_needed(&raw_root, config.zstd_level, &mut segments).await?;
                        let segment = segments.get_mut(&stream)
                            .ok_or_else(|| InfraError::Msg(format!("missing {:?} segment", stream)))?;
                        segment.writer.write_all(&line).await.map_err(InfraError::Io)?;
                    }
                    Some(StorageMessage::Shutdown) | None => break,
                }
            }
            _ = flush_tick.tick() => {
                rotate_if_needed(&raw_root, config.zstd_level, &mut segments).await?;
                for segment in segments.values_mut() {
                    segment.writer.flush().await.map_err(InfraError::Io)?;
                }
                if last_sync.elapsed() >= Duration::from_secs(config.sync_interval_sec) {
                    for segment in segments.values() {
                        segment.writer.get_ref().sync_data().await.map_err(InfraError::Io)?;
                    }
                    last_sync = Instant::now();
                }
            }
        }
    }

    finalize_all(segments, config.zstd_level).await
}

async fn open_all_segments(raw_root: &Path) -> InfraResult<BTreeMap<StorageStream, ActiveSegment>> {
    let mut output = BTreeMap::new();
    for stream in StorageStream::ALL {
        output.insert(stream, open_segment(raw_root, stream).await?);
    }
    Ok(output)
}

async fn open_segment(raw_root: &Path, stream: StorageStream) -> InfraResult<ActiveSegment> {
    let hour_key = utc_hour_key();
    let path = raw_root.join(format!(
        "{}.part-{}.jsonl.partial",
        stream.file_stem(),
        hour_key
    ));
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .await
        .map_err(InfraError::Io)?;
    Ok(ActiveSegment {
        hour_key,
        path,
        writer: BufWriter::new(file),
    })
}

async fn rotate_if_needed(
    raw_root: &Path,
    zstd_level: i32,
    segments: &mut BTreeMap<StorageStream, ActiveSegment>,
) -> InfraResult<()> {
    let current_hour = utc_hour_key();
    if segments
        .values()
        .all(|segment| segment.hour_key == current_hour)
    {
        return Ok(());
    }
    let old = std::mem::take(segments);
    finalize_all(old, zstd_level).await?;
    *segments = open_all_segments(raw_root).await?;
    Ok(())
}

async fn finalize_all(
    segments: BTreeMap<StorageStream, ActiveSegment>,
    zstd_level: i32,
) -> InfraResult<()> {
    for (_, mut segment) in segments {
        segment.writer.flush().await.map_err(InfraError::Io)?;
        segment
            .writer
            .get_ref()
            .sync_data()
            .await
            .map_err(InfraError::Io)?;
        drop(segment.writer);
        let ready = segment.path.with_extension("ready");
        fs::rename(&segment.path, &ready)
            .await
            .map_err(InfraError::Io)?;
        tokio::task::spawn_blocking(move || compress_ready_file(&ready, zstd_level))
            .await
            .map_err(|err| InfraError::Msg(format!("compression task join: {err}")))??;
    }
    Ok(())
}

fn compress_ready_file(path: &Path, zstd_level: i32) -> InfraResult<()> {
    let expected = std_fs::metadata(path).map_err(InfraError::Io)?.len();
    let final_path = path.with_extension("zst");
    let temporary = PathBuf::from(format!("{}.partial", final_path.display()));
    if temporary.exists() {
        std_fs::remove_file(&temporary).map_err(InfraError::Io)?;
    }
    let mut input = BufReader::new(StdFile::open(path).map_err(InfraError::Io)?);
    let output = StdFile::create(&temporary).map_err(InfraError::Io)?;
    zstd::stream::copy_encode(&mut input, output, zstd_level)
        .map_err(|err| InfraError::Msg(format!("compress {}: {err}", path.display())))?;
    StdOpenOptions::new()
        .read(true)
        .open(&temporary)
        .and_then(|file| file.sync_all())
        .map_err(InfraError::Io)?;

    let mut decoder =
        zstd::stream::read::Decoder::new(StdFile::open(&temporary).map_err(InfraError::Io)?)
            .map_err(|err| InfraError::Msg(format!("verify {}: {err}", temporary.display())))?;
    let decoded = io::copy(&mut decoder, &mut io::sink()).map_err(InfraError::Io)?;
    if decoded != expected {
        return Err(InfraError::Msg(format!(
            "zstd size mismatch for {}: expected {expected}, decoded {decoded}",
            path.display()
        )));
    }

    let checksum = sha256_file(&temporary)?;
    std_fs::rename(&temporary, &final_path).map_err(InfraError::Io)?;
    let checksum_path = PathBuf::from(format!("{}.sha256", final_path.display()));
    let checksum_tmp = PathBuf::from(format!("{}.partial", checksum_path.display()));
    let file_name = final_path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| InfraError::Msg("invalid compressed file name".into()))?;
    std_fs::write(&checksum_tmp, format!("{checksum}  {file_name}\n")).map_err(InfraError::Io)?;
    std_fs::rename(checksum_tmp, checksum_path).map_err(InfraError::Io)?;
    std_fs::remove_file(path).map_err(InfraError::Io)
}

fn recover_abandoned_segments(root: &Path, zstd_level: i32) -> InfraResult<()> {
    if !root.exists() {
        return Ok(());
    }
    let mut files = Vec::new();
    collect_files(root, &mut files)?;
    files.sort();
    for path in files.iter().filter(|path| {
        path.extension().and_then(|value| value.to_str()) == Some("partial")
            && path.to_string_lossy().ends_with(".jsonl.partial")
    }) {
        truncate_to_last_newline(path)?;
        let ready = path.with_extension("ready");
        std_fs::rename(path, &ready).map_err(InfraError::Io)?;
        compress_ready_file(&ready, zstd_level)?;
    }
    for path in files
        .iter()
        .filter(|path| path.extension().and_then(|value| value.to_str()) == Some("ready"))
    {
        if path.exists() {
            compress_ready_file(path, zstd_level)?;
        }
    }
    Ok(())
}

fn collect_files(root: &Path, output: &mut Vec<PathBuf>) -> InfraResult<()> {
    for entry in std_fs::read_dir(root).map_err(InfraError::Io)? {
        let path = entry.map_err(InfraError::Io)?.path();
        if path.is_dir() {
            collect_files(&path, output)?;
        } else {
            output.push(path);
        }
    }
    Ok(())
}

fn truncate_to_last_newline(path: &Path) -> InfraResult<()> {
    let mut file = StdOpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .map_err(InfraError::Io)?;
    let mut position = file.metadata().map_err(InfraError::Io)?.len();
    if position == 0 {
        return Ok(());
    }
    let mut buffer = vec![0_u8; 64 * 1024];
    while position > 0 {
        let count = usize::try_from(position.min(buffer.len() as u64))
            .map_err(|err| InfraError::Msg(format!("segment size conversion: {err}")))?;
        let start = position - count as u64;
        file.seek(SeekFrom::Start(start)).map_err(InfraError::Io)?;
        file.read_exact(&mut buffer[..count])
            .map_err(InfraError::Io)?;
        if let Some(index) = buffer[..count].iter().rposition(|byte| *byte == b'\n') {
            file.set_len(start + index as u64 + 1)
                .map_err(InfraError::Io)?;
            return Ok(());
        }
        position = start;
    }
    file.set_len(0).map_err(InfraError::Io)
}

fn sha256_file(path: &Path) -> InfraResult<String> {
    let mut input = BufReader::new(StdFile::open(path).map_err(InfraError::Io)?);
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let count = input.read(&mut buffer).map_err(InfraError::Io)?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

fn utc_hour_key() -> String {
    let now = Utc::now();
    format!(
        "{:04}{:02}{:02}T{:02}",
        now.year(),
        now.month(),
        now.day(),
        now.hour()
    )
}

pub fn manifest_streams() -> Vec<String> {
    StorageStream::ALL
        .into_iter()
        .map(|stream| stream.file_stem().to_string())
        .collect()
}

pub fn schema_version() -> u16 {
    SCHEMA_VERSION
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use serde_json::json;

    use super::*;

    fn test_root(name: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "queue-aware-smdp-{name}-{}-{nonce}",
            std::process::id()
        ))
    }

    fn collector(root: PathBuf) -> CollectorConfig {
        CollectorConfig {
            host_id: "test".into(),
            data_root: root,
            writer_capacity: 16,
            flush_interval_ms: 10,
            sync_interval_sec: 1,
            zstd_level: 1,
            schedule_interval_ms: 100,
            account_snapshot_interval_sec: 30,
            system_snapshot_interval_sec: 30,
        }
    }

    fn manifest(run_id: &str) -> RunManifest {
        RunManifest {
            schema_version: SCHEMA_VERSION,
            run_id: run_id.into(),
            host_id: "test".into(),
            process_id: std::process::id(),
            build_commit: "test".into(),
            process_start_wall_ns: 1,
            config_path: "test.toml".into(),
            probe_enabled: false,
            streams: manifest_streams(),
        }
    }

    #[tokio::test]
    async fn finalizes_compressed_segments_with_checksums() {
        let root = test_root("finalize");
        let config = collector(root.clone());
        let (storage, task) = start_storage(&config, "run-1", &manifest("run-1"))
            .await
            .unwrap();
        storage
            .record(StorageStream::PublicMarket, &json!({"event":"test"}))
            .await
            .unwrap();
        storage.shutdown().await.unwrap();
        task.await.unwrap().unwrap();

        let raw_root = root.join("runs/run-1/raw");
        let public_file = std_fs::read_dir(&raw_root)
            .unwrap()
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .find(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.starts_with("public_market") && name.ends_with(".zst"))
            })
            .unwrap();
        let decoded = zstd::stream::decode_all(StdFile::open(&public_file).unwrap()).unwrap();
        assert_eq!(decoded, b"{\"event\":\"test\"}\n");
        assert!(PathBuf::from(format!("{}.sha256", public_file.display())).exists());
        assert!(raw_root.read_dir().unwrap().all(|entry| {
            !entry
                .unwrap()
                .path()
                .to_string_lossy()
                .ends_with(".partial")
        }));
        std_fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn recovery_truncates_an_incomplete_json_line() {
        let root = test_root("recovery");
        let raw_root = root.join("runs/old/raw");
        std_fs::create_dir_all(&raw_root).unwrap();
        let partial = raw_root.join("public_market.part-old.jsonl.partial");
        std_fs::write(&partial, b"{\"complete\":true}\n{\"partial\":").unwrap();

        recover_abandoned_segments(&root, 1).unwrap();

        let compressed = raw_root.join("public_market.part-old.jsonl.zst");
        let decoded = zstd::stream::decode_all(StdFile::open(compressed).unwrap()).unwrap();
        assert_eq!(decoded, b"{\"complete\":true}\n");
        std_fs::remove_dir_all(root).unwrap();
    }
}
