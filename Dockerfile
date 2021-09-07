FROM rust:latest as builder
RUN apt-get update && apt-get install -y libgdal-dev
WORKDIR /usr/src/myapp
COPY . .
RUN cargo install --path .

FROM debian:buster-slim
RUN apt-get update && apt-get install -y libgdal20 && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/local/cargo/bin/aw3d30-parquet /usr/local/bin/aw3d30-parquet
ENTRYPOINT [ "aw3d30-parquet" ]
CMD ["help"]
