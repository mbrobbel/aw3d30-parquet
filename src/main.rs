use futures::future;
use gdal::{errors::GdalError, Dataset};
use parquet::{
    basic::{self, Compression, Repetition},
    column::writer::ColumnWriter,
    errors::ParquetError,
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
use std::{error::Error, fmt::Debug, path::Path, sync::Arc};
use tokio::{
    fs::{self, File},
    io::{AsyncReadExt, AsyncWriteExt},
    task,
};
use tracing::instrument;
use tracing_subscriber::fmt::format::FmtSpan;

const TIF_DIR: &'static str = "tif";
const PARQUET_DIR: &'static str = "parquet";
const BUCKET: &'static str = "raster";
const PREFIX: &'static str = "AW3D30/AW3D30_global/";
const ENDPOINT: &'static str = "opentopography.s3.sdsc.edu";

#[instrument(err, skip(client, key, size))]
async fn get_object(
    client: &S3Client,
    key: &str,
    size: usize,
) -> Result<Vec<u8>, RusotoError<GetObjectError>> {
    let mut buf = Vec::with_capacity(size);
    client
        .get_object(GetObjectRequest {
            bucket: BUCKET.to_string(),
            key: key.to_string(),
            ..Default::default()
        })
        .await?
        .body
        .expect("body")
        .into_async_read()
        .read_to_end(&mut buf)
        .await?;
    Ok(buf)
}

#[instrument(err, skip(path, buf))]
async fn write_file<T>(path: T, buf: Vec<u8>) -> Result<(), std::io::Error>
where
    T: Debug + AsRef<Path>,
{
    File::create(path.as_ref()).await?.write_all(&buf).await?;
    Ok(())
}

#[instrument(err)]
fn read_file<T>(path: T) -> Result<Vec<((f64, f64), i32)>, GdalError>
where
    T: Debug + AsRef<Path>,
{
    let dataset = Dataset::open(path.as_ref())?;
    let gt = dataset.geo_transform()?;
    let rasterband = dataset.rasterband(1)?;
    Ok(rasterband
        .read_band_as::<i32>()?
        .data
        .chunks_exact(rasterband.x_size())
        .enumerate()
        .flat_map(|(y, line)| {
            line.iter().enumerate().map(move |(x, elevation)| {
                // https://gdal.org/user/raster_data_model.html#affine-geotransform
                let lon = gt[0] + x as f64 * gt[1] + y as f64 * gt[2];
                let lat = gt[3] + x as f64 * gt[4] + y as f64 * gt[5];
                ((lat, lon), *elevation)
            })
        })
        .collect())
}

#[instrument(skip(schema, writer_props, data), err)]
fn write_parquet<T>(
    path: T,
    schema: Arc<Type>,
    writer_props: Arc<WriterProperties>,
    data: (Vec<f64>, Vec<f64>, Vec<i32>),
) -> Result<(), ParquetError>
where
    T: Debug + AsRef<Path>,
{
    let (lat, lon, elevation) = data;
    let mut writer =
        SerializedFileWriter::new(std::fs::File::create(path.as_ref())?, schema, writer_props)?;

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

#[instrument(fields(key = %object.key.as_ref().unwrap()[PREFIX.len()..]), skip(client, object, schema, writer_props))]
async fn process(
    client: S3Client,
    object: Object,
    schema: Arc<Type>,
    writer_props: Arc<WriterProperties>,
) -> Result<(), Box<dyn Error + Send>> {
    let key = object.key.unwrap().to_string();
    let size = object.size.unwrap() as usize;

    let buf = get_object(&client, &key, size)
        .await
        .expect("get_object failed");

    let file_name = Path::new(&key).file_name().unwrap();
    let path = Path::new(TIF_DIR).join(file_name);

    write_file(&path, buf).await.expect("write_file failed");

    let (coordinate, elevation): (Vec<(f64, f64)>, _) =
        task::spawn_blocking(move || read_file(&path).unwrap())
            .await
            .expect("read_file failed")
            .into_iter()
            .unzip();
    let (lat, lon) = coordinate.into_iter().unzip();

    let path = Path::new(PARQUET_DIR)
        .join(file_name)
        .with_extension("parquet");

    task::spawn_blocking(move || {
        write_parquet(&path, schema, writer_props, (lat, lon, elevation)).unwrap()
    })
    .await
    .expect("write_parquet");

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt()
        .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
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

    let mut tasks = Vec::new();
    loop {
        let ListObjectsV2Output {
            contents,
            next_continuation_token,
            is_truncated,
            ..
        } = client.list_objects_v2(req.clone()).await?;

        contents.unwrap().into_iter().for_each(|obj| {
            tasks.push(tokio::spawn(process(
                client.clone(),
                obj,
                schema.clone(),
                writer_props.clone(),
            )));
        });
        req.continuation_token = next_continuation_token;
        if let Some(false) = is_truncated {
            break;
        }
    }

    // Wait for all tasks.
    future::try_join_all(tasks).await?;

    Ok(())
}
