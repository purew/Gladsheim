use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};

mod osm_parser;
mod utils;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Parsing the osm.pbf into basic routing tiles
    ParseOsmToBasicTiles {
        /// The osm-file to parse
        #[arg(long)]
        fname: PathBuf,
        /// A directory to write output files to
        #[arg(long)]
        output_dir: PathBuf,
    },
    /// Builds hub-labels from the basic data built in `ParseOsmToBasicTiles`
    BuildHubLabels {
        /// The basic routing tiles produced in previous step
        #[arg(long)]
        fname: PathBuf,

        /// Routing endpoint used for calculating Hub labels
        /// Needs to be something fast like OSRM to be feasible
        #[arg(long, default_value = "127.0.0.1:5000")]
        directions_endpoint: String,
    },
}

#[derive(Clone, Copy, Debug, Default, Hash, Eq, PartialEq, bincode::Encode, bincode::Decode)]
struct NodeId(i64);

#[derive(Debug, Default, bincode::Encode, bincode::Decode)]
struct WayId(i64);

#[derive(Debug, Default, bincode::Encode, bincode::Decode)]
struct Way {
    id: WayId,
    name: Option<String>,
    is_oneway: bool,
    nodes: Vec<NodeId>,
    polyline: String,
}
#[derive(Debug, Default, bincode::Encode, bincode::Decode)]
struct Edge {
    from: NodeId,
    to: NodeId,
    is_oneway: bool,
    nodes: Vec<NodeId>,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::ParseOsmToBasicTiles { fname, output_dir } => {
            let start_time = std::time::Instant::now();
            let fname_tiles = osm_parser::read_osm_pbf(&fname, &output_dir)?;
            println!(
                "INFO: Finished all parsing in {}ms and produced routing tiles in {}",
                start_time.elapsed().as_millis(),
                &output_dir.display()
            );
            Ok(())
        }
        Commands::BuildHubLabels {
            fname: _,
            directions_endpoint: _,
        } => Ok(()),
    }
}
