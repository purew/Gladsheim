use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use rayon::prelude::*;

mod osm_parser;

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
        /// Parses the osm.pbf
        #[arg(long)]
        fname: PathBuf,
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

#[derive(Clone, Debug, Default, Hash, Eq, PartialEq, bincode::Encode, bincode::Decode)]
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

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::ParseOsmToBasicTiles { fname } => {
            let start_time = std::time::Instant::now();
            let fname_tiles = osm_parser::read_osm_pbf(&fname)?;
            println!(
                "INFO: Finished all parsing in {}ms and produced routing tiles in {}",
                start_time.elapsed().as_millis(),
                fname_tiles.display()
            );
            Ok(())
        }
        Commands::BuildHubLabels {
            fname,
            directions_endpoint,
        } => Ok(()),
    }
}
