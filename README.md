# aw3d30-parquet

Application to download [ALOS World 3D 30 meter DEM](https://www.eorc.jaxa.jp/ALOS/en/aw3d/index_e.htm) data from [OpenTopography](https://opentopography.org/) ([OTALOS.112016.4326.2](https://portal.opentopography.org/raster?opentopoID=OTALOS.112016.4326.2)) and store it as [Parquet](https://parquet.apache.org) data. Used in [Supercomputing for Big Data](https://github.com/abs-tudelft/sbd) course at [Delft University of Technology](https://www.tudelft.nl/).

## Acknowledgements

- _J. Takaku, T. Tadono, K. Tsutsui_
  **Generation of High Resolution Global DSM from ALOS PRISM**
  The International Archives of the Photogrammetry, Remote Sensing and Spatial Information Sciences, pp.243-248, Vol. XL-4, ISPRS TC IV Symposium, Suzhou, China, 2014

- _T. Tadono, H. Ishida, F. Oda, S. Naito, K. Minakawa, H. Iwamoto_
  **Precise Global DEM Generation By ALOS PRISM**
  ISPRS Annals of the Photogrammetry, Remote Sensing and Spatial Information Sciences, pp.71-76, Vol.II-4, 2014

- _Japan Aerospace Exploration Agency (2021)_
  **ALOS World 3D 30 meter DEM. V3.2, Jan 2021.** 
  Distributed by OpenTopography. doi: [10.5069/G94M92HB](https://doi.org/10.5069/G94M92HB)

## Usage

### Requirements

- [Rust](https://rustup.rs) (stable)

### Run

Build and run:

```
cargo run --release
```

GeoTIFF files are downloaded to the `tif` directory and `parquet` files are written to the `parquet` directory.
