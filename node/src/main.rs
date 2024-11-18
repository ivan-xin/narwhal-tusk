// Copyright(C) Facebook, Inc. and its affiliates.

use std::time::Duration;
use alloy_primitives::private::serde;
use anyhow::{Context, Result};
use clap::{crate_name, crate_version, App, AppSettings, ArgMatches, SubCommand};
use config::Export as _;
use config::Import as _;
use config::{Committee, KeyPair, Parameters, WorkerId};
use consensus::Consensus;
use crypto::Digest;
use env_logger::Env;
use log::info;
use primary::{Certificate, Primary};
use store::Store;
use tokio::sync::mpsc::{channel, Receiver};
use tokio::time::sleep;
use worker::Worker;

/// The default channel capacity.
pub const CHANNEL_CAPACITY: usize = 1_000;

#[tokio::main]
async fn main() -> Result<()> {
    let matches = App::new(crate_name!())
        .version(crate_version!())
        .about("A research implementation of Narwhal and Tusk.")
        .args_from_usage("-v... 'Sets the level of verbosity'")
        .subcommand(
            SubCommand::with_name("generate_keys")
                .about("Print a fresh key pair to file")
                .args_from_usage("--filename=<FILE> 'The file where to print the new key pair'"),
        )
        .subcommand(
            SubCommand::with_name("run")
                .about("Run a node")
                .args_from_usage("--keys=<FILE> 'The file containing the node keys'")
                .args_from_usage("--committee=<FILE> 'The file containing committee information'")
                .args_from_usage("--parameters=[FILE] 'The file containing the node parameters'")
                .args_from_usage("--store=<PATH> 'The path where to create the data store'")
                .subcommand(SubCommand::with_name("primary").about("Run a single primary"))
                .subcommand(
                    SubCommand::with_name("worker")
                        .about("Run a single worker")
                        .args_from_usage("--id=<INT> 'The worker id'"),
                )
                .setting(AppSettings::SubcommandRequiredElseHelp),
        )
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .get_matches();

    let log_level = match matches.occurrences_of("v") {
        0 => "error",
        1 => "warn",
        2 => "info",
        3 => "debug",
        _ => "trace",
    };
    let mut logger = env_logger::Builder::from_env(Env::default().default_filter_or(log_level));
    #[cfg(feature = "benchmark")]
    logger.format_timestamp_millis();
    logger.init();

    match matches.subcommand() {
        ("generate_keys", Some(sub_matches)) => KeyPair::new()
            .export(sub_matches.value_of("filename").unwrap())
            .context("Failed to generate key pair")?,
        ("run", Some(sub_matches)) => run(sub_matches).await?,
        _ => unreachable!(),
    }
    Ok(())
}

// Runs either a worker or a primary.
async fn run(matches: &ArgMatches<'_>) -> Result<()> {
    let key_file = matches.value_of("keys").unwrap();
    let committee_file = matches.value_of("committee").unwrap();
    let parameters_file = matches.value_of("parameters");
    let store_path = matches.value_of("store").unwrap();

    // Read the committee and node's keypair from file.
    let keypair = KeyPair::import(key_file).context("Failed to load the node's keypair")?;
    let committee =
        Committee::import(committee_file).context("Failed to load the committee information")?;

    // Load default parameters if none are specified.
    let parameters = match parameters_file {
        Some(filename) => {
            Parameters::import(filename).context("Failed to load the node's parameters")?
        }
        None => Parameters::default(),
    };

    // Make the data store.
    let store = Store::new(store_path).context("Failed to create a store")?;

    // Channels the sequence of certificates.
    let (tx_output, rx_output) = channel(CHANNEL_CAPACITY);

    // Check whether to run a primary, a worker, or an entire authority.
    match matches.subcommand() {
        // Spawn the primary and consensus core.
        ("primary", _) => {
            let (tx_new_certificates, rx_new_certificates) = channel(CHANNEL_CAPACITY);
            let (tx_feedback, rx_feedback) = channel(CHANNEL_CAPACITY);
            Primary::spawn(
                keypair,
                committee.clone(),
                parameters.clone(),
                store,
                /* tx_consensus */ tx_new_certificates,
                /* rx_consensus */ rx_feedback,
            );
            Consensus::spawn(
                committee,
                parameters.gc_depth,
                /* rx_primary */ rx_new_certificates,
                /* tx_primary */ tx_feedback,
                tx_output,
            );
        }

        // Spawn a single worker.
        ("worker", Some(sub_matches)) => {
            let id = sub_matches
                .value_of("id")
                .unwrap()
                .parse::<WorkerId>()
                .context("The worker id must be a positive integer")?;
            Worker::spawn(keypair.name, id, committee, parameters, store);
        }
        _ => unreachable!(),
    }

    // Analyze the consensus' output.
    analyze(rx_output, store_path.parse()?).await;

    // If this expression is reached, the program ends and all other tasks terminate.
    unreachable!();
}

/// Receives an ordered list of certificates and apply any application-specific logic.
async fn analyze(mut rx_output: Receiver<Certificate>, store_path: String) {
    loop {
        tokio::select! {
                Some(certificate) = rx_output.recv() => {
                    info!("rx_output len: {}", rx_output.len());
                    let store_path = store_path.clone();
                    tokio::spawn(async move {
                        handle_cert(certificate, store_path).await.expect("handle_cert panic");
                    });
                },
                else => {
                    println!("loop exit!!!");
                    break;
                },
            }
    }
}
async fn reconstruct_batch(
    digest: Digest,
    worker_id: u32,
    store_path: String,
) -> eyre::Result<Vec<u8>> {
    let max_attempts = 3;
    let backoff_ms = 500;
    let db_path = format!("{}-{}", store_path, worker_id);
    // Open the database to each worker
    let db = rocksdb::DB::open_for_read_only(&rocksdb::Options::default(), db_path, true)?;

    for attempt in 0..max_attempts {
        // Query the db
        let key = digest.to_vec();
        match db.get(&key)? {
            Some(res) => return Ok(res),
            None if attempt < max_attempts - 1 => {
                println!(
                    "digest {} not found, retrying in {}ms",
                    digest,
                    backoff_ms * (attempt + 1)
                );
                sleep(Duration::from_millis(backoff_ms * (attempt + 1))).await;
                continue;
            }
            None => eyre::bail!(
                "digest {} not found after {} attempts",
                digest,
                max_attempts
            ),
        }
    }
    unreachable!()
}
async fn handle_cert(certificate: Certificate, store_path: String) -> eyre::Result<()> {
    // Reconstruct batches from certificate
    let futures: Vec<_> = certificate
        .header
        .payload
        .into_iter()
        .map(|(digest, worker_id)| reconstruct_batch(digest, worker_id, store_path.clone()))
        .collect();

    let batches = futures::future::join_all(futures).await;

    for batch in batches {
        let batch = batch?;
        process_batch(batch).await?;
    }
    Ok(())
}

async fn process_batch(batch: Vec<u8>) -> eyre::Result<()> {
    // Deserialize and process the batch
    match bincode::deserialize(&batch) {
        Ok(WorkerMessage::Batch(txs)) => {
            info!("txs len: {}", txs.len());
            Ok(())
        }
        _ => eyre::bail!("Unrecognized message format"),
    }
}

pub type Transaction = Vec<u8>;
pub type Batch = Vec<Transaction>;
#[derive(serde::Deserialize)]
pub enum WorkerMessage {
    Batch(Batch),
}