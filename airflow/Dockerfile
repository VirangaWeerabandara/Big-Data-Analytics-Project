FROM apache/airflow:3.1.8-python3.12

# Pinning apache-airflow in the same install stops pip from upgrading or
# downgrading Airflow while resolving our package. ward_common has no
# dependencies and talks to Postgres through the psycopg2 driver already
# shipped in this image.
COPY --chown=airflow:root common /tmp/common
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" /tmp/common \
    && rm -rf /tmp/common
