FROM ubuntu:latest

ENV VERSION 0.2.0

WORKDIR /
RUN apt-get update && \
  apt-get install -y curl && \
  curl -L https://github.com/mbrobbel/aw3d30-parquet/releases/download/${VERSION}/aw3d30-parquet-${VERSION}-x86_64-unknown-linux-gnu.tar.gz | tar xz
ENTRYPOINT [ "/aw3d30-parquet" ]
CMD [ "help" ]
