FROM apache/superset:4.0.2

USER root
RUN pip install --no-cache-dir "trino>=0.330" "sqlalchemy-trino>=0.5"
USER superset
