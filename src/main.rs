use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

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

use geo_types::coord;
use osmpbf::{Element, ElementReader};

#[derive(Debug, Default)]
struct Loc {
    nano_lat: i64,
    nano_lon: i64,
}

#[derive(Debug, Default)]
struct NodeId(i64);

#[derive(Debug, Default)]
struct WayId(i64);

#[derive(Debug, Default)]
struct Way {
    id: WayId,
    name: Option<String>,
    is_oneway: bool,
    nodes: Vec<NodeId>,
    //polyline: String,
}

#[derive(Debug, Default)]
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
    fn merge(mut self, other: Self) -> Self {
        Self {
            num_highways: self.num_highways + other.num_highways,
            num_drivable: self.num_drivable + other.num_drivable,
            num_oneways: self.num_oneways + other.num_oneways,
            num_nodes: self.num_nodes + other.num_nodes,
        }
    }
}

#[derive(Debug, Default)]
struct PbfReaderResult {
    stats: StatsParsing,
    ways: Vec<Way>,
    nodes: Vec<(NodeId, Node)>,
}
impl PbfReaderResult {
    fn merge(mut self, other: Self) -> Self {
        self.ways.extend(other.ways);
        self.nodes.extend(other.nodes);
        self.stats = self.stats.merge(other.stats);
        self
    }
}

trait SimpleNode {
    fn nano_lat(&self) -> i64;
    fn nano_lon(&self) -> i64;
    fn id(&self) -> i64;
}
impl SimpleNode for osmpbf::dense::DenseNode<'_> {
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

fn read_osm_pbf(osm_pbf: &Path) -> Result<()> {
    let start_time = std::time::Instant::now();
    let reader = ElementReader::from_path(osm_pbf)
        .with_context(|| format!("Failed loading {}", osm_pbf.display()))?;

    let parsed = reader.par_map_reduce(
        |element| match element {
            Element::Way(way) => {
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
                                println!("WARN: Unknown oneway value: {}", value)
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
                        //polyline,
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

                    ways,
                    nodes: Vec::with_capacity(0),
                }
            }
            Element::Node(node) => parse_node(node),
            Element::DenseNode(node) => parse_node(node),
            Element::Relation(_relation) => PbfReaderResult::default(),
        },
        || PbfReaderResult::default(),
        |a, b| a.merge(b),
    )?;

    println!(
        "Parsed {} in {}ms",
        osm_pbf.display(),
        start_time.elapsed().as_millis()
    );
    println!("Stats: {:#?}", parsed.stats);
    Ok(())
}

fn parse_node<T: SimpleNode>(node: T) -> PbfReaderResult {
    let nodes = vec![(
        NodeId(node.id()),
        Node {
            loc: Loc {
                nano_lat: node.nano_lat(),
                nano_lon: node.nano_lon(),
            },
        },
    )];

    PbfReaderResult {
        nodes,
        stats: StatsParsing {
            num_highways: 0,
            num_drivable: 0,
            num_oneways: 0,
            num_nodes: 1,
        },
        ..Default::default()
    }
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Parse { fname } => read_osm_pbf(&fname),
    }
}
