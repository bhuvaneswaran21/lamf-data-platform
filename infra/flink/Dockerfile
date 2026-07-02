FROM flink:1.18.1-scala_2.12-java11

ARG ICEBERG_VERSION=1.5.2
ARG FLINK_MINOR=1.18
ARG KAFKA_SQL_VERSION=3.1.0-1.18


RUN apt-get update && apt-get install -y python3 python3-pip && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    pip3 install --no-cache-dir apache-flink==1.18.1 redis==5.0.7

WORKDIR /opt/flink/lib
RUN set -eux; M=https://repo1.maven.org/maven2; \
    curl -fSL -o iceberg-flink-runtime.jar \
      $M/org/apache/iceberg/iceberg-flink-runtime-${FLINK_MINOR}/${ICEBERG_VERSION}/iceberg-flink-runtime-${FLINK_MINOR}-${ICEBERG_VERSION}.jar; \
    curl -fSL -o iceberg-aws-bundle.jar \
      $M/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar; \
    curl -fSL -o flink-sql-connector-kafka.jar \
      $M/org/apache/flink/flink-sql-connector-kafka/${KAFKA_SQL_VERSION}/flink-sql-connector-kafka-${KAFKA_SQL_VERSION}.jar
