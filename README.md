# Data Engineering & Big Data Bootcamp Final Project

## Environment Setup

### Creating a Dataproc Cluster

To create a Dataproc cluster, you should select the image in the **Versioning** section as shown below.

```bash
2.0 (Debian 10, Hadoop 3.2, Spark 3.1)
First released on 1/22/2021.
```

You can use the following command to create a Dataproc cluster.
```bash
gcloud dataproc clusters create jupyter-cluster \
  --region=southamerica-east1 \
  --zone=southamerica-east1-a \
  --master-machine-type=n1-standard-2 \
  --master-boot-disk-size=100GB \
  --num-workers=2 \
  --worker-machine-type=n1-standard-2 \
  --worker-boot-disk-size=100GB \
  --image-version=2.1-debian11 \
  --optional-components=JUPYTER \
  --enable-component-gateway \
  --project=YOUR_PROJECT_NAME

```

### Creating a Kafka Cluster
You can create a Kafka cluster by creating a standard VM instance on GCP and running the following commands inside it.

```bash
sudo apt update
sudo apt upgrade -y
sudo apt install docker.io -y
sudo apt install docker-compose -y

nano docker-compose.yml # Create the following docker-compose.yml file.
```


You can use the following command to create a Kafka cluster.
 
```yaml
version: "3.9"
services:
  zookeeper:
    image: zookeeper:3.8.0
    container_name: zookeeper-docker
    hostname: zookeeper # The hostname is intended for use in Kafka to enable communication with ZooKeeper's container-internal name.
    ports:
      - "2181:2181"
    networks:
      - kafka_network

  kafka-server-1:
    image: "bitnami/kafka:3.3.1"
    container_name: kafka-container-1
    hostname: kafka-1
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 0
      KAFKA_CFG_LISTENERS: "PLAINTEXT://:9092,PLAINTEXT_HOST://:9093"  
      KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP: "PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT"
      # Eğer VM makine üzerinde çalıştırılıyorsa localhost yerine external ip adresi yazılmalıdır.
      # KAFKA_CFG_ADVERTISED_LISTENERS: "PLAINTEXT://YOUR KAFKA VM EXTERNAL IP:9092,PLAINTEXT_HOST://localhost:9093"
      KAFKA_CFG_ADVERTISED_LISTENERS: "PLAINTEXT://YOUR KAFKA VM EXTERNAL IP:9092,PLAINTEXT_HOST://kafka-1:9093" 
      KAFKA_CFG_ZOOKEEPER_CONNECT: zookeeper:2181/kafka-1
      ALLOW_PLAINTEXT_LISTENER: "yes"
    networks:
      - kafka_network      
  kafka-ui:
    container_name: kafka-ui
    image: provectuslabs/kafka-ui:master
    ports:
      - 8080:8080 # Changed to avoid port clash with akhq
    depends_on:
      - kafka-server-1
    environment:
      KAFKA_CLUSTERS_0_NAME: local
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka-1:9093
      DYNAMIC_CONFIG_ENABLED: 'true'
    networks:
      - kafka_network

networks:
  kafka_network:
    driver: bridge
```

After pasting and saving, you can start the Kafka cluster by running the following command.
 
```bash
sudo docker-compose up -d
```

### Airflow Installation

You can install Airflow by creating a VM instance with 8GB RAM on GCP and running the following commands inside it.

```bash
sudo apt update
sudo apt upgrade -y
sudo apt install docker.io -y
sudo apt install docker-compose -y

nano docker-compose.yml # Create the following docker-compose.yml file..
```

You can use the following command to create an Airflow cluster.

```yaml
---
version: '3.8'
x-airflow-common:
  &airflow-common
  image: ${AIRFLOW_IMAGE_NAME:-apache/airflow:2.7.3}
  environment:
    &airflow-common-env
    AIRFLOW__CORE__EXECUTOR: CeleryExecutor
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
    AIRFLOW__CORE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
    AIRFLOW__CELERY__RESULT_BACKEND: db+postgresql://airflow:airflow@postgres/airflow
    AIRFLOW__CELERY__BROKER_URL: redis://:@redis:6379/0
    AIRFLOW__CORE__FERNET_KEY: ''
    AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: 'true'
    AIRFLOW__CORE__LOAD_EXAMPLES: 'false' # değişti
    AIRFLOW__API__AUTH_BACKENDS: 'airflow.api.auth.backend.basic_auth,airflow.api.auth.backend.session'
    AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL: '30' # değişti
    AIRFLOW__CORE__TEST_CONNECTION: "Enabled" # değişti
    AIRFLOW__SCHEDULER__ENABLE_HEALTH_CHECK: 'true'
    _PIP_ADDITIONAL_REQUIREMENTS: ${_PIP_ADDITIONAL_REQUIREMENTS:-}
  volumes:
    - ${AIRFLOW_PROJ_DIR:-.}/dags:/opt/airflow/dags
    - ${AIRFLOW_PROJ_DIR:-.}/logs:/opt/airflow/logs
    - ${AIRFLOW_PROJ_DIR:-.}/config:/opt/airflow/config
    - ${AIRFLOW_PROJ_DIR:-.}/plugins:/opt/airflow/plugins
  user: "${AIRFLOW_UID:-50000}:0"
  depends_on:
    &airflow-common-depends-on
    redis:
      condition: service_healthy
    postgres:
      condition: service_healthy
services:
  postgres:
    image: postgres:13
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    volumes:
      - postgres-db-volume:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "airflow"]
      interval: 10s
      retries: 5
      start_period: 5s
    restart: always
  redis:
    image: redis:latest
    expose:
      - 6379
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 10s
      timeout: 30s
      retries: 50
      start_period: 30s
    restart: always
  airflow-webserver:
    <<: *airflow-common
    command: webserver
    ports:
      - "8080:8080"
    healthcheck:
      test: ["CMD", "curl", "--fail", "http://localhost:8080/health"]
      interval: 30s
      timeout: 10s
      retries: 5
      start_period: 30s
    restart: always
    depends_on:
      <<: *airflow-common-depends-on
      airflow-init:
        condition: service_completed_successfully
  airflow-scheduler:
    <<: *airflow-common
    command: scheduler
    healthcheck:
      test: ["CMD", "curl", "--fail", "http://localhost:8974/health"]
      interval: 30s
      timeout: 10s
      retries: 5
      start_period: 30s
    restart: always
    depends_on:
      <<: *airflow-common-depends-on
      airflow-init:
        condition: service_completed_successfully
  airflow-worker:
    <<: *airflow-common
    command: celery worker
    healthcheck:
      test:
        - "CMD-SHELL"
        - 'celery --app airflow.providers.celery.executors.celery_executor.app inspect ping -d "celery@$${HOSTNAME}" || celery --app airflow.executors.celery_executor.app inspect ping -d "celery@$${HOSTNAME}"'
      interval: 30s
      timeout: 10s
      retries: 5
      start_period: 30s
    environment:
      <<: *airflow-common-env
      DUMB_INIT_SETSID: "0"
    restart: always
    depends_on:
      <<: *airflow-common-depends-on
      airflow-init:
        condition: service_completed_successfully
  airflow-triggerer:
    <<: *airflow-common
    command: triggerer
    healthcheck:
      test: ["CMD-SHELL", 'airflow jobs check --job-type TriggererJob --hostname "$${HOSTNAME}"']
      interval: 30s
      timeout: 10s
      retries: 5
      start_period: 30s
    restart: always
    depends_on:
      <<: *airflow-common-depends-on
      airflow-init:
        condition: service_completed_successfully
  airflow-init:
    <<: *airflow-common
    entrypoint: /bin/bash
    command:
      - -c
      - |
        function ver() {
          printf "%04d%04d%04d%04d" $${1//./ }
        }
        airflow_version=$$(AIRFLOW__LOGGING__LOGGING_LEVEL=INFO && gosu airflow airflow version)
        airflow_version_comparable=$$(ver $${airflow_version})
        min_airflow_version=2.2.0
        min_airflow_version_comparable=$$(ver $${min_airflow_version})
        if (( airflow_version_comparable < min_airflow_version_comparable )); then
          echo
          echo -e "\033[1;31mERROR!!!: Too old Airflow version $${airflow_version}!\e[0m"
          echo "The minimum Airflow version supported: $${min_airflow_version}. Only use this or higher!"
          echo
          exit 1
        fi
        if [[ -z "${AIRFLOW_UID}" ]]; then
          echo
          echo -e "\033[1;33mWARNING!!!: AIRFLOW_UID not set!\e[0m"
          echo "If you are on Linux, you SHOULD follow the instructions below to set "
          echo "AIRFLOW_UID environment variable, otherwise files will be owned by root."
          echo "For other operating systems you can get rid of the warning with manually created .env file:"
          echo "    See: https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html#setting-the-right-airflow-user"
          echo
        fi
        one_meg=1048576
        mem_available=$$(($$(getconf _PHYS_PAGES) * $$(getconf PAGE_SIZE) / one_meg))
        cpus_available=$$(grep -cE 'cpu[0-9]+' /proc/stat)
        disk_available=$$(df / | tail -1 | awk '{print $$4}')
        warning_resources="false"
        if (( mem_available < 4000 )) ; then
          echo
          echo -e "\033[1;33mWARNING!!!: Not enough memory available for Docker.\e[0m"
          echo "At least 4GB of memory required. You have $$(numfmt --to iec $$((mem_available * one_meg)))"
          echo
          warning_resources="true"
        fi
        if (( cpus_available < 2 )); then
          echo
          echo -e "\033[1;33mWARNING!!!: Not enough CPUS available for Docker.\e[0m"
          echo "At least 2 CPUs recommended. You have $${cpus_available}"
          echo
          warning_resources="true"
        fi
        if (( disk_available < one_meg * 10 )); then
          echo
          echo -e "\033[1;33mWARNING!!!: Not enough Disk space available for Docker.\e[0m"
          echo "At least 10 GBs recommended. You have $$(numfmt --to iec $$((disk_available * 1024 )))"
          echo
          warning_resources="true"
        fi
        if [[ $${warning_resources} == "true" ]]; then
          echo
          echo -e "\033[1;33mWARNING!!!: You have not enough resources to run Airflow (see above)!\e[0m"
          echo "Please follow the instructions to increase amount of resources available:"
          echo "   https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html#before-you-begin"
          echo
        fi
        mkdir -p /sources/logs /sources/dags /sources/plugins
        chown -R "${AIRFLOW_UID}:0" /sources/{logs,dags,plugins}
        exec /entrypoint airflow version
    environment:
      <<: *airflow-common-env
      _AIRFLOW_DB_MIGRATE: 'true'
      _AIRFLOW_WWW_USER_CREATE: 'true'
      _AIRFLOW_WWW_USER_USERNAME: ${_AIRFLOW_WWW_USER_USERNAME:-airflow}
      _AIRFLOW_WWW_USER_PASSWORD: ${_AIRFLOW_WWW_USER_PASSWORD:-airflow}
      _PIP_ADDITIONAL_REQUIREMENTS: ''
    user: "0:0"
    volumes:
      - ${AIRFLOW_PROJ_DIR:-.}:/sources

  airflow-cli:
    <<: *airflow-common
    profiles:
      - debug
    environment:
      <<: *airflow-common-env
      CONNECTION_CHECK_MAX_COUNT: "0"
    command:
      - bash
      - -c
      - airflow
  flower:
    <<: *airflow-common
    command: celery flower
    profiles:
      - flower
    ports:
      - "5555:5555"
    healthcheck:
      test: ["CMD", "curl", "--fail", "http://localhost:5555/"]
      interval: 30s
      timeout: 10s
      retries: 5
      start_period: 30s
    restart: always
    depends_on:
      <<: *airflow-common-depends-on
      airflow-init:
        condition: service_completed_successfully
volumes:
  postgres-db-volume:
```
After pasting and saving, you can start the Airflow cluster by running the following command.
 
```bash
echo -e "AIRFLOW_UID=$(id -u)" > .env
sudo docker-compose up airflow-init
sudo docker-compose up -d
sudo docker-compose down
```


Project Code
Kafka Producer
You can send messages to Kafka by using the following code inside the producer.py file.

```python
import json
from kafka import KafkaProducer
import pandas as pd
import time

bootstrap_servers = 'YOUR KAFKA VM EXTERNAL IP:9092'
topic_name = 'test'

producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda x: json.dumps(x).encode("utf-8")
)

df = pd.read_csv("YOUR gsutil URL")
df = df.drop(columns=["Order ID"])
for index, row in df.iterrows():
    to_json = row.to_dict()
    print(to_json)
    producer.send(topic_name, value=to_json)
    time.sleep(1)
```

### Spark to BigQuery

You can transfer data from Spark to BigQuery by using the following code inside the "spark_to_bigquery.py" file.
```python
# pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType, FloatType



# spark = SparkSession.builder.appName("Streaming").getOrCreate()
spark = (SparkSession.builder
         .appName("MyApp")
         .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2")
         .config("spark.jars", "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar")
         .getOrCreate())

# You can use the following code to check whether the Kafka and BigQuery configurations have been loaded.

spark.sparkContext.getConf().getAll()

bucket = "YOUR BUCKET NAME" 
spark.conf.set("temporaryGcsBucket", bucket) 
spark.conf.set("parentProject", "YOUR PROJECT NAME")

schema = StructType([
    StructField("Product", StringType(), True),
    StructField("Quantity_Ordered", StringType(), True),
    StructField("Price_Each", StringType(), True),
    StructField("Order_Date", StringType(), True),
    StructField("Purchase_Address", StringType(), True)
])


kafkaDF = spark.readStream.format("kafka").option(
    "kafka.bootstrap.servers", "YOUR KAFKA VM EXTERNAL IP:9092").option(
    "subscribe", "test").load()

activationDF = kafkaDF.select(from_json(kafkaDF["value"].cast("string"), schema).alias("activation")).select("activation.*")

# Convert the Order\_Date column to date format. 12/30/19 00:01 -> 2019-12-30 00:01:00

df = (activationDF
      .withColumn("Quantity_Ordered", col("Quantity_Ordered").cast(IntegerType()))
      .withColumn("Price_Each", col("Price_Each").cast(FloatType()))
      .withColumn("Order_Date", to_timestamp(col("Order_Date"), "MM/dd/yy HH:mm")))

# Price_Each choose bigger than 100
df = df.filter(col("Price_Each") > 10)

# model_write_console = df.writeStream.outputMode("append").format("console").start().awaitTermination()

save_data = df.writeStream\
    .outputMode("append").format("bigquery")\
    .option("table", "YOUR BIGQUERY DATASET NAME.sales")\
    .option("checkpointLocation", "/path/to/checkpoint/dir/in/hdfs")\
    .option("failOnDataLoss", False)\
    .option("truncate", False)\
    .start().awaitTermination()
```

### Airflow DAG

`airflow_dag.py` You can automate the Spark to BigQuery process with Airflow by using the following code inside the file.
 
```python
from airflow import DAG
import pendulum
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
PROJE_AD = "YOUR PROJECT NAME"
DB_AD = "BIGQUERY DATASET NAME"

dag =  DAG(
    dag_id="18_GCSToBigQuery",
    schedule="@daily",
    start_date=pendulum.datetime(2025,6,9,tz="UTC"),
    catchup=False,
    )

sorgu =f"Select * from {PROJE_AD}.{DB_AD}.sales where Price_Each > 1000"

create_new_table = BigQueryExecuteQueryOperator(
        task_id = "create_new_table",
        sql=sorgu,
        destination_dataset_table=f"{PROJE_AD}.{DB_AD}.data_analysis",
        create_disposition="CREATE_IF_NEEDED", 
        write_disposition="WRITE_TRUNCATE",#  WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY
        use_legacy_sql=False,
        gcp_conn_id="google_cloud_default",
        dag=dag
    )
create_new_table
