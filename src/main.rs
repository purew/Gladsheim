use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    /// Optional name to operate on
    //name: Option<String>,

    ///// Sets a custom config file
    //#[arg(short, long, value_name = "FILE")]
    //config: Option<PathBuf>,

    ///// Turn debugging information on
    //#[arg(short, long, action = clap::ArgAction::Count)]
    //debug: u8,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Parsing the osm.pbf
    Parse {
        /// Parses the osm.pbf
        #[arg(long)]
        fname: PathBuf,
    },
}

use osmpbf::{Element, ElementReader};

fn read_osm_pbf(osm_pbf: &Path) -> Result<()> {
    let reader = ElementReader::from_path(osm_pbf)
        .with_context(|| format!("Failed loading {}", osm_pbf.display()))?;

    // Count the ways
    let ways = reader.par_map_reduce(
        |element| match element {
            Element::Way(_) => 1,
            _ => 0,
        },
        || 0_u64,     // Zero is the identity value for addition
        |a, b| a + b, // Sum the partial results
    )?;

    println!("Number of ways: {ways}");
    Ok(())
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Parse { fname } => read_osm_pbf(&fname),
    }
}
