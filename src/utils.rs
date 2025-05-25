use std::f64::consts::PI;

use anyhow::{bail, Result};

#[derive(Debug)]
pub struct TileCoord {
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
