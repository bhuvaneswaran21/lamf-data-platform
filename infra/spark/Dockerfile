FROM apache/spark:3.5.1-scala2.12-java11-python3-ubuntu

USER root
ARG ICEBERG_VERSION=1.5.2
ARG NESSIE_VERSION=0.99.0
ARG SCALA_VERSION=2.12
ARG SPARK_VERSION=3.5
WORKDIR /opt/spark/jars

RUN set -eux; \
    M=https://repo1.maven.org/maven2; \
    curl -fSL -o iceberg-spark-runtime.jar \
      $M/org/apache/iceberg/iceberg-spark-runtime-${SPARK_VERSION}_${SCALA_VERSION}/${ICEBERG_VERSION}/iceberg-spark-runtime-${SPARK_VERSION}_${SCALA_VERSION}-${ICEBERG_VERSION}.jar; \
    curl -fSL -o iceberg-aws-bundle.jar \
      $M/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar; \
    curl -fSL -o nessie-spark-extensions.jar \
      $M/org/projectnessie/nessie-integrations/nessie-spark-extensions-${SPARK_VERSION}_${SCALA_VERSION}/${NESSIE_VERSION}/nessie-spark-extensions-${SPARK_VERSION}_${SCALA_VERSION}-${NESSIE_VERSION}.jar

COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf

RUN pip install --no-cache-dir faker==25.2.0 redis==5.0.7

USER spark
