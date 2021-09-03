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
use rusoto_core::{
    credential::{AwsCredentials, StaticProvider},
    HttpClient, Region, RusotoError,
};
use rusoto_s3::{
    GetObjectError, GetObjectRequest, ListObjectsV2Output, ListObjectsV2Request, Object, S3Client,
    S3,
};
use std::{
    error::Error,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::{
    fs::{self, File},
    task,
};
use tracing::instrument;
use tracing_subscriber::fmt::format::FmtSpan;

const TIF_DIR: &str = "tif";
const PARQUET_DIR: &str = "parquet";
const BUCKET: &str = "raster";
const PREFIX: &str = "AW3D30/AW3D30_global/";
const ENDPOINT: &str = "opentopography.s3.sdsc.edu";

#[instrument(err, skip(client))]
async fn download_object(
    client: S3Client,
    key: String,
) -> Result<PathBuf, RusotoError<GetObjectError>> {
    let path = Path::new(TIF_DIR).join(Path::new(&key).file_name().unwrap());
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
    Ok(path)
}

#[instrument(fields(key = %path.file_stem().unwrap().to_str().unwrap()), skip(path, schema, writer_props), err)]
fn write_parquet(
    path: PathBuf,
    schema: Arc<Type>,
    writer_props: Arc<WriterProperties>,
) -> Result<(), Box<dyn std::error::Error>> {
    let dataset = Dataset::open(path.as_ref())?;
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

    let path = Path::new(PARQUET_DIR)
        .join(path.file_stem().unwrap())
        .with_extension("parquet");
    let mut writer = SerializedFileWriter::new(std::fs::File::create(path)?, schema, writer_props)?;
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
            ColumnWriter::Int32ColumnWriter(ref mut c) => c.write_batch(&elevation, None, None)?,
            _ => unreachable!(),
        };
        row_writer.close_column(col_writer)?;
    }
    writer.close_row_group(row_writer)?;
    writer.close()?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt()
        .with_span_events(FmtSpan::CLOSE)
        .init();

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

    // Make sure output dirs exists.
    fs::create_dir_all(TIF_DIR).await?;
    fs::create_dir_all(PARQUET_DIR).await?;

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
        WriterProperties::builder().set_compression(Compression::ZSTD),
    ));

    let mut objects = Vec::default();
    loop {
        let ListObjectsV2Output {
            contents,
            next_continuation_token,
            is_truncated,
            ..
        } = client.list_objects_v2(req.clone()).await?;

        // Collect all objects keys.
        if let Some(contents) = contents {
            objects.extend(contents.into_iter().map(|Object { key, .. }| key.unwrap()))
        }

        // Fetch next object when needed.
        req.continuation_token = next_continuation_token;
        if let Some(false) = is_truncated {
            break;
        }
    }

    stream::iter(objects)
        .map(|key| task::spawn(download_object(client.clone(), key)))
        .buffer_unordered(50)
        .try_for_each_concurrent(None, |path| {
            let path = path.unwrap();
            let schema = schema.clone();
            let writer_props = writer_props.clone();
            task::spawn_blocking(move || {
                write_parquet(path, schema, writer_props).unwrap();
            })
        })
        .await?;

    Ok(())
}
