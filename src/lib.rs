use anyhow::{Context, Result, anyhow, bail};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::{fs, path::Path};

pub const IPC_SCHEMA_VERSION: u32 = 1;
pub const WORK_PLAN_SCHEMA_VERSION: u32 = 1;
pub const FEE_BUCKET_SECONDS: i64 = 10;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub gridpool_url: String,
    pub adapter_token_file: String,
    pub socket_path: String,
    #[serde(default)]
    pub ckpool_notify_socket: Option<String>,
    pub queue_database: String,
    pub health_listen: String,
    pub expected_network_id: String,
    pub expected_bitcoin_network: String,
    pub minimum_protocol_version: i32,
    #[serde(default = "default_maximum_message_bytes")]
    pub maximum_message_bytes: usize,
    #[serde(default = "default_poll_interval_ms")]
    pub poll_interval_ms: u64,
    #[serde(default = "default_maximum_plan_age_seconds")]
    pub maximum_plan_age_seconds: u64,
    #[serde(default)]
    pub fee_basis_points: u16,
    pub fee_secret_file: String,
    #[serde(default = "default_source_instance")]
    pub source_instance: String,
}

fn default_maximum_message_bytes() -> usize {
    262_144
}
fn default_poll_interval_ms() -> u64 {
    1_000
}
fn default_maximum_plan_age_seconds() -> u64 {
    30
}
fn default_source_instance() -> String {
    "ckpool".to_owned()
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let raw =
            fs::read_to_string(path).with_context(|| format!("read config {}", path.display()))?;
        let config: Self = toml::from_str(&raw).context("parse adapter config")?;
        if config.fee_basis_points > 10_000 {
            bail!("fee_basis_points must be between 0 and 10000");
        }
        if config.maximum_message_bytes < 65_536 {
            bail!("maximum_message_bytes must be at least 65536");
        }
        if config.maximum_plan_age_seconds < 15 {
            bail!("maximum_plan_age_seconds must be at least 15");
        }
        Ok(config)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkPlan {
    pub schema_version: u32,
    pub plan_id: String,
    pub sequence: i64,
    pub network_id: String,
    pub bitcoin_network: String,
    pub protocol_version: i32,
    pub active_snapshot_id: String,
    pub current_state_id: String,
    pub candidate_state_id: String,
    pub current_tip_block_hash: Option<String>,
    pub current_tip_block_height: Option<i64>,
    pub mining_work_safe: bool,
    pub mining_work_safety_reason: String,
    pub provisional_tip_block_hash: Option<String>,
    pub total_payout_slot_count: usize,
    pub shared_winner_slot_count: usize,
    pub support_fee_enabled: bool,
    pub coinbase_output_count: usize,
    pub coinbase_tx_outputs_bytes: usize,
    pub coinbase_tx_outputs_hex: String,
    pub coinbase_outputs: Vec<CoinbaseOutput>,
    pub minimum_accepted_difficulty: f64,
    pub minimum_pulse_difficulty: f64,
    pub minimum_difficulty_to_enter_reserve: f64,
    pub minimum_difficulty_to_enter_reserve_display: String,
    pub user_identifier_rule: String,
    pub mode: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CoinbaseOutput {
    pub value: u64,
    pub address: String,
    pub script_pub_key_hex: String,
    pub output_hex: String,
    pub username: String,
    pub difficulty: f64,
    pub diff_string: String,
}

impl WorkPlan {
    pub fn validate(&self, config: &Config) -> Result<()> {
        if self.schema_version != WORK_PLAN_SCHEMA_VERSION {
            bail!("unsupported work-plan schema {}", self.schema_version);
        }
        if self.network_id != config.expected_network_id {
            bail!("GridPool network mismatch: {}", self.network_id);
        }
        if self.bitcoin_network != config.expected_bitcoin_network {
            bail!("Bitcoin network mismatch: {}", self.bitcoin_network);
        }
        if self.protocol_version < config.minimum_protocol_version {
            bail!(
                "GridPool protocol {} is below required {}",
                self.protocol_version,
                config.minimum_protocol_version
            );
        }
        if !self.mining_work_safe {
            bail!(
                "GridPool reports unsafe mining work: {}",
                self.mining_work_safety_reason
            );
        }
        if self.plan_id.len() != 64 || hex::decode(&self.plan_id).is_err() {
            bail!("invalid plan ID");
        }
        if self.active_snapshot_id.is_empty()
            || self.current_tip_block_hash.as_deref().unwrap_or("").len() != 64
        {
            bail!("work plan is missing snapshot or current parent");
        }
        if !self.minimum_pulse_difficulty.is_finite() || self.minimum_pulse_difficulty < 1.0 {
            bail!("invalid pulse difficulty");
        }
        let serialized = hex::decode(&self.coinbase_tx_outputs_hex)
            .context("decode serialized payout outputs")?;
        if serialized.len() != self.coinbase_tx_outputs_bytes {
            bail!("serialized payout output length mismatch");
        }
        if self.coinbase_output_count != self.coinbase_outputs.len() {
            bail!("payout output count mismatch");
        }
        let parsed_count = parse_tx_output_vector(&serialized)?;
        if parsed_count != self.coinbase_output_count {
            bail!(
                "serialized payout vector contains {parsed_count} outputs, expected {}",
                self.coinbase_output_count
            );
        }
        let concatenated = self
            .coinbase_outputs
            .iter()
            .map(|output| hex::decode(&output.output_hex).context("decode payout output"))
            .collect::<Result<Vec<_>>>()?
            .concat();
        let (_, prefix_len) = read_compact_size(&serialized, 0)?;
        if serialized[prefix_len..] != concatenated {
            bail!("serialized payout vector does not match listed outputs");
        }
        Ok(())
    }
}

fn parse_tx_output_vector(data: &[u8]) -> Result<usize> {
    let (count, mut offset) = read_compact_size(data, 0)?;
    let count = usize::try_from(count).context("output count overflow")?;
    for _ in 0..count {
        if data.len().saturating_sub(offset) < 8 {
            bail!("truncated payout value");
        }
        offset += 8;
        let (script_len, next) = read_compact_size(data, offset)?;
        offset = next;
        let script_len = usize::try_from(script_len).context("script length overflow")?;
        offset = offset
            .checked_add(script_len)
            .ok_or_else(|| anyhow!("script length overflow"))?;
        if offset > data.len() {
            bail!("truncated payout script");
        }
    }
    if offset != data.len() {
        bail!("trailing bytes in payout vector");
    }
    Ok(count)
}

fn read_compact_size(data: &[u8], offset: usize) -> Result<(u64, usize)> {
    let first = *data
        .get(offset)
        .ok_or_else(|| anyhow!("missing compact size"))?;
    match first {
        0x00..=0xfc => Ok((u64::from(first), offset + 1)),
        0xfd => read_le(data, offset + 1, 2),
        0xfe => read_le(data, offset + 1, 4),
        0xff => read_le(data, offset + 1, 8),
    }
}

fn read_le(data: &[u8], offset: usize, width: usize) -> Result<(u64, usize)> {
    let end = offset
        .checked_add(width)
        .ok_or_else(|| anyhow!("compact size overflow"))?;
    let bytes = data
        .get(offset..end)
        .ok_or_else(|| anyhow!("truncated compact size"))?;
    let mut value = 0u64;
    for (shift, byte) in bytes.iter().enumerate() {
        value |= u64::from(*byte) << (shift * 8);
    }
    Ok((value, end))
}

pub fn fee_bucket(
    secret: &[u8],
    network: &str,
    parent_hash: &str,
    payout_script_hex: &str,
    unix_seconds: i64,
    basis_points: u16,
) -> Result<(i64, bool)> {
    if basis_points > 10_000 {
        bail!("fee basis points exceed 10000");
    }
    let bucket = unix_seconds.div_euclid(FEE_BUCKET_SECONDS);
    let mut mac = Hmac::<Sha256>::new_from_slice(secret).context("invalid fee secret")?;
    mac.update(network.as_bytes());
    mac.update(&[0]);
    mac.update(parent_hash.as_bytes());
    mac.update(&[0]);
    mac.update(payout_script_hex.as_bytes());
    mac.update(&[0]);
    mac.update(&bucket.to_be_bytes());
    let bytes = mac.finalize().into_bytes();
    let sample = u64::from_be_bytes(bytes[..8].try_into().expect("HMAC is at least eight bytes"));
    Ok((bucket, sample % 10_000 < u64::from(basis_points)))
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum IpcRequest {
    GetPlan {
        schema_version: u32,
    },
    FeeDecision {
        schema_version: u32,
        parent_hash: String,
        payout_script_hex: String,
        unix_seconds: i64,
    },
    SubmitProof {
        schema_version: u32,
        proof: serde_json::Value,
    },
    SubmitTelemetry {
        schema_version: u32,
        batch: serde_json::Value,
    },
    RecordShare {
        schema_version: u32,
        channel_id: String,
        payout_address: String,
        username: String,
        accepted: bool,
        difficulty: f64,
        fee_work: bool,
        observed_unix_ms: i64,
    },
    Health {
        schema_version: u32,
    },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum IpcResponse {
    Ok {
        schema_version: u32,
        data: serde_json::Value,
    },
    Error {
        schema_version: u32,
        code: String,
        message: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fee_schedule_tracks_basis_points() {
        let secret = [7u8; 32];
        let selected = (0..200_000)
            .filter(|bucket| {
                fee_bucket(
                    &secret,
                    "mainnet",
                    &"01".repeat(32),
                    "0014abcd",
                    bucket * FEE_BUCKET_SECONDS,
                    150,
                )
                .unwrap()
                .1
            })
            .count();
        let ratio = selected as f64 / 200_000f64;
        assert!((ratio - 0.015).abs() < 0.001, "ratio={ratio}");
    }

    #[test]
    fn fee_schedule_is_stable_for_identity_and_bucket() {
        let first = fee_bucket(
            &[3u8; 32],
            "mainnet",
            &"02".repeat(32),
            "0014cafe",
            123_456,
            150,
        )
        .unwrap();
        let second = fee_bucket(
            &[3u8; 32],
            "mainnet",
            &"02".repeat(32),
            "0014cafe",
            123_459,
            150,
        )
        .unwrap();
        assert_eq!(first, second);
    }

    #[test]
    fn full_gridpool_suffix_exceeds_legacy_eight_kibibyte_buffers() {
        let mut encoded = vec![0xfd, 0x2b, 0x01]; // CompactSize(299)
        for index in 0..299u64 {
            encoded.extend_from_slice(&(1_000 + index).to_le_bytes());
            encoded.push(22);
            encoded.extend_from_slice(&[0x00, 0x14]);
            encoded.extend_from_slice(&index.to_le_bytes());
            encoded.extend_from_slice(&[0x42; 12]);
        }

        assert!(encoded.len() > 8 * 1024);
        assert_eq!(parse_tx_output_vector(&encoded).unwrap(), 299);
    }
}
