use anyhow::{Context, Result};
use clap::Parser;

mod engine;
use engine::TransactionEngine;

#[derive(Parser)]
struct Cli {
    #[clap(parse(from_os_str))]
    transaction_file: std::path::PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Cli::parse();

    // Create reader from file
    let reader = csv::ReaderBuilder::new()
        .delimiter(b',')
        .flexible(true)
        .trim(csv::Trim::All)
        .from_path(args.transaction_file)
        .context("Unable to read transaction file.")?;

    // Initialize tx engine
    let engine = TransactionEngine::new();

    // Process all records
    engine.process_records(reader.into_deserialize()).await?;

    // Write output to stdout
    write_output_csv(&engine)?;

    Ok(())
}

fn write_output_csv(engine: &TransactionEngine) -> Result<()> {
    // Build CSV writer
    let mut writer = csv::WriterBuilder::new()
        .delimiter(b',')
        .has_headers(true)
        .flexible(false)
        .from_writer(std::io::stdout());

    // Serialize all account records
    for account in engine.accounts()? {
        writer.serialize(account)?;
    }

    Ok(())
}
