use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use rayon::prelude::*;

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

#[derive(Clone, Debug, Default, bincode::Encode, bincode::Decode)]
struct Loc {
    //nano_lat: i64,
    //nano_lon: i64,
    lat: f64,
    lon: f64,
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

#[derive(Clone, Debug, Default, bincode::Encode, bincode::Decode)]
struct Node {
    loc: Loc,
}
#[derive(Debug, Default)]
struct StatsParsing {
    num_highways: usize,
    num_drivable: usize,
    num_oneways: usize,
    num_nodes: usize,
}
impl StatsParsing {
    fn merge(self, other: Self) -> Self {
        Self {
            num_highways: self.num_highways + other.num_highways,
            num_drivable: self.num_drivable + other.num_drivable,
            num_oneways: self.num_oneways + other.num_oneways,
            num_nodes: self.num_nodes + other.num_nodes,
        }
    }
}
#[derive(Debug, Default, bincode::Encode, bincode::Decode)]
struct Map {
    ways: Vec<Way>,
    nodes: Vec<(NodeId, Node)>,
}
impl Map {
    fn merge(mut self, other: Self) -> Self {
        self.ways.extend(other.ways);
        self.nodes.extend(other.nodes);
        self
    }
}

#[derive(Debug, Default)]
struct PbfReaderResult {
    stats: StatsParsing,
    map: Map,
}
impl PbfReaderResult {
    fn merge(mut self, other: Self) -> Self {
        self.stats = self.stats.merge(other.stats);
        self.map = self.map.merge(other.map);
        self
    }
}

trait SimpleNode {
    fn lat(&self) -> f64;
    fn lon(&self) -> f64;
    fn nano_lat(&self) -> i64;
    fn nano_lon(&self) -> i64;
    fn id(&self) -> i64;
}
impl SimpleNode for osmpbf::dense::DenseNode<'_> {
    fn lat(&self) -> f64 {
        self.lat()
    }
    fn lon(&self) -> f64 {
        self.lon()
    }
    fn nano_lat(&self) -> i64 {
        self.nano_lat()
    }
    fn nano_lon(&self) -> i64 {
        self.nano_lon()
    }
    fn id(&self) -> i64 {
        self.id()
    }
}
impl SimpleNode for osmpbf::elements::Node<'_> {
    fn lat(&self) -> f64 {
        self.lat()
    }
    fn lon(&self) -> f64 {
        self.lat()
    }
    fn nano_lat(&self) -> i64 {
        self.nano_lat()
    }
    fn nano_lon(&self) -> i64 {
        self.nano_lat()
    }
    fn id(&self) -> i64 {
        self.id()
    }
}

fn read_osm_pbf(osm_pbf: &Path) -> Result<()> {
    let start_time = std::time::Instant::now();
    let reader = ElementReader::from_path(osm_pbf)
        .with_context(|| format!("Failed loading {}", osm_pbf.display()))?;

    let mut parsed_ways = reader.par_map_reduce(
        |element| match element {
            Element::Way(way) => parse_way(&way),
            Element::Node(node) => PbfReaderResult::default(),
            Element::DenseNode(node) => PbfReaderResult::default(),
            Element::Relation(_relation) => PbfReaderResult::default(),
        },
        || PbfReaderResult::default(),
        |a, b| a.merge(b),
    )?;

    println!(
        "INFO: Finished first parsing in {}ms",
        start_time.elapsed().as_millis()
    );
    println!("Stats: {:#?}", parsed_ways.stats);
    let start_time = std::time::Instant::now();
    let active_nodes = parsed_ways
        .map
        .ways
        .iter()
        .map(|way| way.nodes.clone())
        .flatten()
        .collect::<HashSet<_>>();
    println!(
        "INFO: Collected active nodes in {}ms",
        start_time.elapsed().as_millis()
    );

    let start_time = std::time::Instant::now();
    let reader = ElementReader::from_path(osm_pbf)
        .with_context(|| format!("Failed loading {}", osm_pbf.display()))?;

    let parsed_nodes = reader.par_map_reduce(
        |element| match element {
            Element::Way(_) => PbfReaderResult::default(),
            Element::Node(node) => parse_node(node, &active_nodes),
            Element::DenseNode(node) => parse_node(node, &active_nodes),
            Element::Relation(_relation) => PbfReaderResult::default(),
        },
        || PbfReaderResult::default(),
        |a, b| a.merge(b),
    )?;
    println!(
        "INFO: Finished second parsing in {}ms",
        start_time.elapsed().as_millis()
    );
    println!(
        "Total number of nodes: {}k",
        parsed_nodes.stats.num_nodes / 1000
    );
    println!(
        "Number of parsed nodes: {}k",
        parsed_nodes.map.nodes.len() / 1000
    );

    // Next, populate the polylines of the ways
    let node_table = parsed_nodes
        .map
        .nodes
        .iter()
        .cloned()
        .collect::<HashMap<_, _>>();
    parsed_ways.map.ways.par_iter_mut().for_each(|way| {
        let coords = way
            .nodes
            .iter()
            .filter_map(|node_id| match node_table.get(&node_id) {
                Some(node) => Some(geo_types::coord! {
                    x: node.loc.lon,
                    y: node.loc.lat,
                }),
                None => {
                    println!("ERR: Could not find node");
                    None
                }
            });
        let line_string: geo_types::LineString<f64> = coords.collect();
        match polyline::encode_coordinates(line_string, 6) {
            Ok(polyline) => {
                //println!("DEBUG: Polyline {}", polyline);
                way.polyline = polyline;
            }
            Err(err) => {
                println!("ERR: Failed creating polyline: {:?}", err);
            }
        }
    });

    let combined_ways_and_nodes = Map {
        ways: parsed_ways.map.ways,
        nodes: parsed_nodes.map.nodes,
    };

    let fname = {
        let mut fname = PathBuf::from("/tmp/");
        fname.push(osm_pbf.file_name().context("Missing filename")?);
        fname
    };
    println!("INFO: Writing to {}", fname.display());
    let start_time = std::time::Instant::now();
    let mut file = std::fs::File::create(&fname)
        .with_context(|| format!("Failed opening file {}", fname.display()))?;
    bincode::encode_into_std_write(
        combined_ways_and_nodes,
        &mut file,
        bincode::config::standard(),
    )
    .with_context(|| format!("Failed writing to file {}", fname.display()))?;
    println!(
        "INFO: Finished writing to filen in {}ms",
        start_time.elapsed().as_millis()
    );

    Ok(())
}

fn parse_way(way: &osmpbf::Way) -> PbfReaderResult {
    let mut is_drivable = false;
    let mut name = None;
    let mut is_oneway = false;
    for (key, value) in way.tags() {
        match key {
            // https://wiki.openstreetmap.org/wiki/Key:highway
            "highway" => match value {
                // Main tags
                "motorway" => {
                    is_drivable = true;
                }
                "trunk" => {
                    is_drivable = true;
                }
                "primary" => {
                    is_drivable = true;
                }
                "secondary" => {
                    is_drivable = true;
                }
                "tertiary" => {
                    is_drivable = true;
                }
                "unclassified" => {
                    is_drivable = true;
                }
                "residential" => {
                    is_drivable = true;
                }
                // Link roads
                "motorway_link" => {
                    is_drivable = true;
                }
                "trunk_link" => {
                    is_drivable = true;
                }
                "primary_link" => {
                    is_drivable = true;
                }
                "secondary_link" => {
                    is_drivable = true;
                }
                "tertiary_link" => {
                    is_drivable = true;
                }
                // Special road types
                "living_street" => {}
                "service" => {}
                "pedestrian" => {}
                "track" => {}
                "bus_guideway" => {}
                "escape" => {}
                "raceway" => {}
                "road" => {}
                "busway" => {}
                _ => {
                    //println!("Unhandled highway value: {}", value);
                }
            },
            "name" => {
                name = Some(value.to_string());
            }
            "oneway" => match value {
                "yes" => is_oneway = true,
                "no" => {}
                _ => {
                    //println!("WARN: Unknown oneway value: {}", value)
                }
            },

            _ => {}
        }
    }

    let ways = if is_drivable {
        //let polyline = {
        //    let coords = way.node_locations().into_iter().map(|way_node| {
        //        println!("Lat {} lon {}", way_node.lat(), way_node.lon());
        //        coord! {x: way_node.lon(), y: way_node.lat()}
        //    });
        //    let line_string: geo_types::LineString<f64> = coords.collect();
        //    match polyline::encode_coordinates(line_string, 6) {
        //        Ok(polyline) => polyline,
        //        Err(err) => {
        //            println!("ERR: Failed creating polyline: {:?}", err);
        //            "".into()
        //        }
        //    }
        //};
        //if !polyline.is_empty() {
        //    println!("Polyline {}", polyline);
        //}

        let nodes = way
            .refs()
            .into_iter()
            .map(|node_id| NodeId(node_id))
            .collect::<Vec<_>>();
        vec![Way {
            id: WayId(way.id()),
            name,
            is_oneway,
            nodes,
            polyline: "".into(),
        }]
    } else {
        Vec::with_capacity(0)
    };

    PbfReaderResult {
        stats: StatsParsing {
            num_highways: 1,
            num_drivable: if is_drivable { 1 } else { 0 },
            num_oneways: if is_oneway { 1 } else { 0 },
            num_nodes: 0,
        },

        map: Map {
            ways,
            nodes: Vec::with_capacity(0),
        },
    }
}

fn parse_node<T: SimpleNode>(node: T, nodes_of_interest: &HashSet<NodeId>) -> PbfReaderResult {
    let node_id = NodeId(node.id());

    let nodes = if nodes_of_interest.contains(&node_id) {
        vec![(
            node_id,
            Node {
                loc: Loc {
                    lat: node.lat(),
                    lon: node.lon(),
                    //nano_lat: node.nano_lat(),
                    //nano_lon: node.nano_lon(),
                },
            },
        )]
    } else {
        Vec::with_capacity(0)
    };

    PbfReaderResult {
        map: Map {
            nodes,
            ..Default::default()
        },
        stats: StatsParsing {
            num_highways: 0,
            num_drivable: 0,
            num_oneways: 0,
            num_nodes: 1,
        },
    }
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Parse { fname } => {
            let start_time = std::time::Instant::now();
            let result = read_osm_pbf(&fname);
            println!(
                "INFO: Finished all parsing in {}ms",
                start_time.elapsed().as_millis()
            );
            result
        }
    }
}
