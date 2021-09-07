use futures::{stream, StreamExt, TryStreamExt};
use gdal::Dataset;
use parquet::{
    basic::{self, Compression, Repetition},
    column::writer::ColumnWriter,
    file::{
        properties::{WriterProperties, WriterPropertiesBuilder},
        writer::{FileWriter, SerializedFileWriter},
    },
    schema::types::Type,
};
use regex::{Captures, Regex};
use rusoto_core::{
    credential::{AwsCredentials, StaticProvider},
    HttpClient, Region, RusotoError,
};
use rusoto_s3::{
    GetObjectError, GetObjectRequest, ListObjectsV2Output, ListObjectsV2Request, Object, S3Client,
    S3,
};
use std::{
    convert::TryFrom,
    error::Error,
    path::{Path, PathBuf},
    sync::Arc,
};
use structopt::StructOpt;
use tokio::{
    fs::{self, File},
    task,
};
use tracing::{event, instrument, Level};
use tracing_subscriber::fmt::format::FmtSpan;

const TIF_DIR: &str = "tif";
const PARQUET_DIR: &str = "parquet";
const BUCKET: &str = "raster";
const PREFIX: &str = "AW3D30/AW3D30_global/";
const ENDPOINT: &str = "opentopography.s3.sdsc.edu";

/// Download ALOS World 3D 30 meter DEM GeoTIFFs and convert them to Parquet
#[derive(StructOpt)]
struct Opt {
    /// Output dir for GeoTIFF files
    #[structopt(short = "t", long = "tif", default_value = TIF_DIR)]
    tif_dir: PathBuf,

    /// Output dir for Parquet files
    #[structopt(short = "p", long = "parquet", default_value = PARQUET_DIR)]
    parquet_dir: PathBuf,

    #[structopt(subcommand)]
    set: Set,
}

#[derive(Copy, Clone, Debug, StructOpt)]
enum Set {
    /// Prepare data for the Netherlands (Requires ~300MB disk space)
    Netherlands,
    /// Prepare data for Europe
    Europe,
    /// Prepare data for the World (Requires ~400GB disk space)
    World,
}

impl Set {
    fn filter(&self, coordinate: Coordinate) -> bool {
        match self {
            Self::Netherlands => {
                matches!(coordinate.lat, Lat::North(ref y) if (50..=53).contains(y))
                    && matches!(coordinate.lon, Lon::East(ref x) if (3..=7).contains(x))
            }
            Self::Europe => {
                matches!(coordinate.lat, Lat::North(ref y) if (23..=80).contains(y))
                    && (matches!(coordinate.lon, Lon::West(x) if x <= 25)
                        || matches!(coordinate.lon, Lon::East(x) if x <= 49))
            }
            Self::World => true,
        }
    }
}

#[derive(Copy, Clone)]
struct Coordinate {
    lat: Lat,
    lon: Lon,
}

#[derive(Copy, Clone)]
enum Lat {
    South(u8),
    North(u8),
}

#[derive(Copy, Clone)]
enum Lon {
    East(u8),
    West(u8),
}

impl<'a> TryFrom<Captures<'a>> for Coordinate {
    type Error = &'static str;

    fn try_from(cap: Captures) -> Result<Self, Self::Error> {
        cap.name("lat")
            .and_then(|y| y.as_str().parse().ok())
            .and_then(|y| match cap.name("y").map(|y| y.as_str()) {
                Some("N") => Some(Lat::North(y)),
                Some("S") => Some(Lat::South(y)),
                _ => None,
            })
            .and_then(|lat| {
                cap.name("lon")
                    .and_then(|x| x.as_str().parse().ok())
                    .and_then(|x| {
                        match cap.name("x").map(|y| y.as_str()) {
                            Some("E") => Some(Lon::East(x)),
                            Some("W") => Some(Lon::West(x)),
                            _ => None,
                        }
                        .map(|lon| Coordinate { lat, lon })
                    })
            })
            .ok_or("bad input")
    }
}

#[instrument(err, skip(client, size, tif_dir))]
async fn download_object(
    client: S3Client,
    key: String,
    size: u64,
    tif_dir: PathBuf,
) -> Result<PathBuf, RusotoError<GetObjectError>> {
    let path = tif_dir.join(Path::new(&key).file_name().unwrap());
    // Skip when file already exists (also check size).
    if path.exists() && path.metadata().unwrap().len() == size {
        event!(Level::WARN, "Skipping download. File already exists.");
    } else {
        let mut file = File::create(&path).await?;
        let mut bytes = client
            .get_object(GetObjectRequest {
                bucket: BUCKET.to_string(),
                key,
                ..Default::default()
            })
            .await?
            .body
            .unwrap()
            .into_async_read();
        tokio::io::copy(&mut bytes, &mut file).await?;
    }
    Ok(path)
}

#[instrument(fields(key = %input_path.file_stem().unwrap().to_str().unwrap()), skip(input_path, output_path, schema, writer_props), err)]
fn write_parquet(
    input_path: PathBuf,
    output_path: PathBuf,
    schema: Arc<Type>,
    writer_props: Arc<WriterProperties>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Skip existing files.
    if !output_path.exists() {
        let dataset = Dataset::open(input_path.as_ref())?;
        let gt = dataset.geo_transform()?;
        let rasterband = dataset.rasterband(1)?;
        let capacity = rasterband.x_size() * rasterband.y_size();
        let mut lat = Vec::with_capacity(capacity);
        let mut lon = Vec::with_capacity(capacity);
        let mut elevation = Vec::with_capacity(capacity);
        rasterband
            .read_band_as::<i32>()?
            .data
            .chunks_exact(rasterband.x_size())
            .enumerate()
            .for_each(|(y, line)| {
                line.iter().enumerate().for_each(|(x, elev)| {
                    // https://gdal.org/user/raster_data_model.html#affine-geotransform
                    lon.push(gt[0] + x as f64 * gt[1] + y as f64 * gt[2]);
                    lat.push(gt[3] + x as f64 * gt[4] + y as f64 * gt[5]);
                    elevation.push(*elev);
                });
            });

        let mut writer =
            SerializedFileWriter::new(std::fs::File::create(output_path)?, schema, writer_props)?;
        let mut row_writer = writer.next_row_group()?;
        if let Some(mut col_writer) = row_writer.next_column()? {
            match col_writer {
                ColumnWriter::DoubleColumnWriter(ref mut c) => c.write_batch(&lat, None, None)?,
                _ => unreachable!(),
            };
            row_writer.close_column(col_writer)?;
        }
        if let Some(mut col_writer) = row_writer.next_column()? {
            match col_writer {
                ColumnWriter::DoubleColumnWriter(ref mut c) => c.write_batch(&lon, None, None)?,
                _ => unreachable!(),
            };
            row_writer.close_column(col_writer)?;
        }
        if let Some(mut col_writer) = row_writer.next_column()? {
            match col_writer {
                ColumnWriter::Int32ColumnWriter(ref mut c) => {
                    c.write_batch(&elevation, None, None)?
                }
                _ => unreachable!(),
            };
            row_writer.close_column(col_writer)?;
        }
        writer.close_row_group(row_writer)?;
        writer.close()?;
    } else {
        event!(Level::WARN, "Skipping Parquet. File already exists.",);
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt()
        .with_span_events(FmtSpan::CLOSE)
        .init();

    let Opt {
        set,
        tif_dir,
        parquet_dir,
    } = Opt::from_args();
    event!(Level::INFO, "Preparing data for {:?}", set);

    event!(
        Level::INFO,
        "GeoTIFF data will be written to `{}`",
        &tif_dir.display()
    );
    fs::create_dir_all(&tif_dir).await?;

    event!(
        Level::INFO,
        "Parquet data data will be written to `{}`",
        &parquet_dir.display()
    );
    fs::create_dir_all(&parquet_dir).await?;

    event!(Level::INFO, "Connecting to OpenTopology server");
    // Create a client that connects to the OpenTopography MinIO storage server.
    let client = S3Client::new_with(
        HttpClient::new()?,
        StaticProvider::from(AwsCredentials::default()),
        Region::Custom {
            name: String::new(),
            endpoint: ENDPOINT.to_string(),
        },
    );

    // List all objects for AW3D30.
    let mut req = ListObjectsV2Request {
        bucket: BUCKET.to_string(),
        prefix: Some(PREFIX.to_string()),
        ..Default::default()
    };

    // Setup parquet write info.
    let coordinate_type = |name: &str| {
        Arc::new(
            Type::primitive_type_builder(name, basic::Type::DOUBLE)
                .with_repetition(Repetition::REQUIRED)
                .build()
                .unwrap(),
        )
    };
    let schema = Arc::new(
        Type::group_type_builder("schema")
            .with_fields(&mut vec![
                coordinate_type("lat"),
                coordinate_type("lon"),
                Arc::new(
                    Type::primitive_type_builder("elevation", basic::Type::INT32)
                        .with_repetition(Repetition::REQUIRED)
                        .build()?,
                ),
            ])
            .build()?,
    );
    let writer_props = Arc::new(WriterPropertiesBuilder::build(
        WriterProperties::builder().set_compression(Compression::SNAPPY),
    ));

    let re = Regex::new(r"ALPSMLC30_(?P<y>[NS])(?P<lat>\d{3})(?P<x>[EW])(?P<lon>\d{3})_DSM")?;
    // todo(mb): create list of objects based on set instead of filtering fetched object list
    let mut objects = Vec::default();
    loop {
        event!(Level::INFO, "Listing objects");
        let ListObjectsV2Output {
            contents,
            next_continuation_token,
            is_truncated,
            ..
        } = client.list_objects_v2(req.clone()).await?;

        // Collect all objects keys.
        if let Some(contents) = contents {
            objects.extend(
                contents
                    .into_iter()
                    .map(|Object { key, size, .. }| (key.unwrap(), size.unwrap() as u64))
                    .filter(|(key, _)| {
                        re.captures(key)
                            .and_then(|cap| Coordinate::try_from(cap).ok())
                            .filter(|&coordinate| set.filter(coordinate))
                            .is_some()
                    }),
            )
        }

        // Fetch next object when needed.
        req.continuation_token = next_continuation_token;
        if let Some(false) = is_truncated {
            event!(Level::INFO, "Listed all objects");
            break;
        }
    }

    event!(Level::INFO, "Downloading {} files", objects.len());
    stream::iter(objects)
        .map(|(key, size)| task::spawn(download_object(client.clone(), key, size, tif_dir.clone())))
        .buffer_unordered(1)
        .try_for_each_concurrent(None, |path| {
            let input_path = path.unwrap();
            let schema = schema.clone();
            let writer_props = writer_props.clone();
            let output_path = parquet_dir
                .join(input_path.file_stem().unwrap())
                .with_extension("parquet");
            task::spawn_blocking(move || {
                write_parquet(input_path, output_path, schema, writer_props).unwrap();
            })
            // todo(mb): (optionally) remove downloaded tif files
        })
        .await?;

    event!(Level::INFO, "Done");

    Ok(())
}
