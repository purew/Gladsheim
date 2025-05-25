use std::{
    collections::{HashMap, HashSet},
    f64::consts::PI,
    hash::{DefaultHasher, Hash, Hasher},
    sync::Mutex,
};

use anyhow::{Result, bail};
use bincode::Encode;

use crate::{Edge, NodeId, Way, WayId, utils};

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub(crate) struct Quadkey(pub(crate) String);

#[derive(Debug, Default, Encode)]
pub(crate) struct Tile {
    pub(crate) edges: Vec<Edge>,
}
#[derive(Debug)]
pub(crate) struct TileCoord {
    pub x: u32,
    pub y: u32,
    pub zoom: u8,
}
pub(crate) fn lat_lon_to_tile_coord(lat: f64, lon: f64, zoom: u8) -> Result<TileCoord> {
    if lat < -85.05112878 || lat > 85.05112878 {
        bail!("Latitude {} out of valid range", lat);
    }
    if lon < -180.0 || lon > 180.0 {
        bail!("Longitude {} out of valid range", lat);
    }

    let n = 2.0f64.powi(zoom as i32);

    // Convert longitude to tile x
    let x = ((lon + 180.0) / 360.0 * n).floor() as u32;

    // Convert latitude to tile y
    let lat_rad = lat * PI / 180.0;
    let y = ((1.0 - (lat_rad.tan() + 1.0 / lat_rad.cos()).ln() / PI) / 2.0 * n).floor() as u32;

    // Clamp to valid tile ranges
    let max_tile = (1 << zoom) - 1;
    let x = x.min(max_tile);
    let y = y.min(max_tile);

    Ok(TileCoord { x, y, zoom })
}
pub(crate) fn tile_coord_to_quadkey(tile: &TileCoord) -> String {
    let mut quadkey = String::new();

    for i in (0..tile.zoom).rev() {
        let mut digit = 0;
        let mask = 1u32 << i;

        if (tile.x & mask) != 0 {
            digit += 1;
        }
        if (tile.y & mask) != 0 {
            digit += 2;
        }

        quadkey.push(std::char::from_digit(digit, 10).unwrap());
    }

    quadkey
}

pub(crate) fn lat_lon_to_quadkey(lat: f64, lon: f64, zoom: u8) -> Result<String> {
    let tile = lat_lon_to_tile_coord(lat, lon, zoom)?;
    Ok(tile_coord_to_quadkey(&tile))
}

/// A structure for allowing a multithreaded producer to inject
/// edges into quadkey buckets with minimal lock contention
pub(crate) struct ParallelQuadkeyMap {
    /// A pre-allocated hashmap where buckets are mutex protected hashmaps
    /// So that we can distribute lock-contention over the buckets
    buckets: HashMap<usize, Mutex<HashMap<Quadkey, Tile>>>,
}

impl ParallelQuadkeyMap {
    const NUM_BUCKETS: usize = 100; // Picked out of thin air
    pub(crate) fn new() -> Self {
        let mut buckets = HashMap::new();
        for bucket_idx in 0..Self::NUM_BUCKETS {
            buckets.insert(bucket_idx, Mutex::new(HashMap::new()));
        }
        Self { buckets }
    }
    pub(crate) fn insert(&self, quadkey: Quadkey, edge: Edge) {
        let bucket_idx: usize = {
            let mut s = DefaultHasher::new();
            quadkey.hash(&mut s);
            s.finish() as usize % Self::NUM_BUCKETS
        };
        let bucket = self
            .buckets
            .get(&bucket_idx)
            // If this lookup fails, program state is invalid, so unwrap is ok
            .unwrap();

        let mut table = bucket
            .lock()
            // Poisoned mutex implied segfault in other part of code, avoid handling this for now
            .unwrap();
        let tile = table.entry(quadkey).or_insert(Tile::default());
        tile.edges.push(edge);
    }

    /// Collects into the final data
    /// FIXME: Just implement the iterator trait, no need to build a Vec
    pub(crate) fn collect(self) -> Vec<(Quadkey, Tile)> {
        let mut vec = Vec::with_capacity(Self::NUM_BUCKETS * 1000);
        for mutex_protected_bucket in self.buckets.into_values() {
            let table = mutex_protected_bucket
                .into_inner()
                // Destructuring the mutex to get inner value. No point in handling poisoned mutex, better to just unwrap and exit program if
                // this occurs
                .unwrap();

            for (quadkey, tile) in table.into_iter() {
                vec.push((quadkey, tile));
            }
        }
        vec
    }
}

//// Next, populate the polylines of the ways
//parsed_ways.map.ways.par_iter_mut().for_each(|way| {
//    let coords = way
//        .nodes
//        .iter()
//        .filter_map(|node_id| match node_table.get(&node_id) {
//            Some(node) => Some(geo_types::coord! {
//                x: node.loc.lon,
//                y: node.loc.lat,
//            }),
//            None => {
//                println!("ERR: Could not find node");
//                None
//            }
//        });
//    let line_string: geo_types::LineString<f64> = coords.collect();
//    match polyline::encode_coordinates(line_string, 6) {
//        Ok(polyline) => {
//            //println!("DEBUG: Polyline {}", polyline);
//            way.polyline = polyline;
//        }
//        Err(err) => {
//            println!("ERR: Failed creating polyline: {:?}", err);
//        }
//    }
//});
