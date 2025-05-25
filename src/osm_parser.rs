use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use osmpbf::{Element, ElementReader};
use rayon::prelude::*;

use crate::{Edge, NodeId, Way, WayId, utils};
use utils::{ParallelQuadkeyMap, Quadkey};

#[derive(Clone, Debug, Default, bincode::Encode, bincode::Decode)]
struct Loc {
    //nano_lat: i64,
    //nano_lon: i64,
    lat: f64,
    lon: f64,
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

pub(crate) fn read_osm_pbf(osm_pbf: &Path, output_tile_dir: &Path) -> Result<()> {
    let start_time = std::time::Instant::now();
    let reader = ElementReader::from_path(osm_pbf)
        .with_context(|| format!("Failed loading {}", osm_pbf.display()))?;

    let mut parsed_ways = reader.par_map_reduce(
        |element| match element {
            Element::Way(way) => parse_way(&way),
            Element::Node(_node) => PbfReaderResult::default(),
            Element::DenseNode(_node) => PbfReaderResult::default(),
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

    let node_table = {
        let start_time = std::time::Instant::now();
        let table = parsed_nodes
            .map
            .nodes
            .iter()
            .cloned()
            .collect::<HashMap<_, _>>();
        println!(
            "INFO: Constructed node lookup table in {}ms",
            start_time.elapsed().as_millis()
        );
        table
    };

    let tiles = {
        // Next, time to detect intersections and split ways into edges
        let start_time = std::time::Instant::now();
        let mut intersection_nodes = HashSet::new();
        {
            let mut seen_nodes = HashSet::new();
            for way in &parsed_ways.map.ways {
                for node_id in &way.nodes {
                    if seen_nodes.contains(&node_id) {
                        intersection_nodes.insert(*node_id);
                    } else {
                        seen_nodes.insert(node_id);
                    }
                }
            }
            println!(
                "INFO: Calculated intersections in {}ms",
                start_time.elapsed().as_millis()
            );
        }

        {
            // Now, use intersections to split ways into edges
            // Multithreaded off-course
            let start_time = std::time::Instant::now();
            let collector = utils::ParallelQuadkeyMap::new();
            let edges = parsed_ways
                .map
                .ways
                .par_iter_mut()
                .flat_map(|way| {
                    let mut initial_node_index_on_edge = 0;
                    let mut new_edges = Vec::new();
                    for (node_index, node_id) in way.nodes.iter().enumerate() {
                        if node_index==0 {
                            // Nothing to cut on first index
                        } else {
                            if intersection_nodes.contains(node_id) {
                                // We've reached an intersection and need to create an edge consisting of the
                                // nodes leading up to this node
                                let from = way.nodes[initial_node_index_on_edge];
                                let to = way.nodes[node_index];
                                let nodes = way.nodes[initial_node_index_on_edge..node_index].to_vec();
                                if nodes.is_empty() {
                                    println!("WARN: Produced edge with empty nodes. initial_node {initial_node_index_on_edge}, node_idx {node_index} from {}", way.id.0);
                                } else {
                                    new_edges.push(crate::Edge {
                                        from,
                                        to,
                                        nodes,
                                        is_oneway: way.is_oneway,
                                    });
                                }
                                initial_node_index_on_edge = node_index;
                            }
                        }
                    }
                    new_edges
                })
                // Next, while we still have a parallel iterator, lets also do the assignment into Z7
                // tiles
                .for_each(|edge| {
                    let node_id = edge
                        .nodes
                        .first()
                        // It's invalid to have an edge without nodes so unwrap is ok here
                        .unwrap();
                    let node = node_table
                        .get(node_id)
                        // Program is invalid if the table misses this node, so unwrap is ok
                        .unwrap();
                    match utils::lat_lon_to_quadkey(node.loc.lat, node.loc.lon, 7) {
                        Ok(s) => {
                            let quadkey = Quadkey(s);
                            collector.insert(quadkey, edge);
                        }
                        Err(err) => {
                            println!("ERROR: Could not create quadkey: {}", err);
                        }
                    }
                });

            let tiles = collector.collect();
            let num_edges: usize = tiles.iter().map(|(_quadkey, tile)| tile.edges.len()).sum();

            println!(
                "INFO: Split {}k ways into {}k edges and produced {} tiles in {}ms",
                parsed_ways.map.ways.len() / 1000,
                num_edges / 1000,
                tiles.len(),
                start_time.elapsed().as_millis()
            );
            tiles
        }
    };

    {
        // Finally write tiles to disk
        let start_time = std::time::Instant::now();
        let _results = tiles
            .par_iter()
            .map(|(quadkey, tile)| -> Result<()> {
                let fname = {
                    let mut fname = output_tile_dir.to_owned();
                    fname.push(&quadkey.0);
                    fname.set_extension("grt");
                    fname
                };
                //println!("INFO: Writing to {}", fname.display());
                let mut file = std::fs::File::create(&fname)
                    .with_context(|| format!("Failed opening file {}", fname.display()))?;
                bincode::encode_into_std_write(tile, &mut file, bincode::config::standard())
                    .with_context(|| format!("Failed writing to file {}", fname.display()))?;
                Ok(())
            })
            .collect::<Vec<_>>();

        println!(
            "INFO: Finished writing to files in {}ms",
            start_time.elapsed().as_millis()
        );
    }
    Ok(())
}

pub(crate) fn parse_way(way: &osmpbf::Way) -> PbfReaderResult {
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

pub(crate) fn parse_node<T: SimpleNode>(
    node: T,
    nodes_of_interest: &HashSet<NodeId>,
) -> PbfReaderResult {
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
