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

## Install

### Docker

You can build a Docker image that runs the application:

```
docker build -t aw3d30 https://github.com/mbrobbel/aw3d30-parquet.git#main
```

Run a container to download and write Parquet files:

```
docker run -it --rm -v `pwd`:/io aw3d30 -t /io/tif -p /io/parquet <set>
```

The following pre-defined sets are supported:

* `netherlands` (requires ~300 MiB of disk space)
* `europe` (~43 GiB)
* `world` (~400 GiB)


## Build

### Requirements

- [Rust](https://rustup.rs) (stable)
- [GDAL](https://gdal.org/index.html) development files

### Cargo

```
cargo install --git https://github.com/mbrobbel/aw3d30-parquet.git
```

Or clone the repository and run:

```
cargo install --path .
```

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or [MIT license](LICENSE-MIT) at your option.

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.
