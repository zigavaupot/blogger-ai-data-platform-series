![Bronze Layer: Spark Structured Streaming, consuming the tfl-arrivals OCI Streaming topic, parsing the JSON payload, adding event-time partitions and ingestion metadata, and appending everything into the tfl.bronze.arrivals_bronze Delta table, feeding the AI Data Platform's silver layer next in the series](https://zigavaupot.github.io/blogger-ai-data-platform-series/bronze-layer-spark-structured-streaming/images/bronze-layer.png)

# Bronze Layer: Spark Structured Streaming (Part 4 of the AI Data Platform Series)

*This is the fourth post in the AI Data Platform series, following [Just Streams: Real-Time Data Pipelines on OCI](https://zigavaupot.blogspot.com/2026/08/ai-data-platform-series-just-streams.html) (the series intro), [Setting Up the AI Data Platform Environment](https://zigavaupot.blogspot.com/2026/09/ai-data-platform-series-setting-up-ai.html) (stage 0), and [OCI Streaming and the Stream Producer](https://zigavaupot.blogspot.com/2026/09/ai-data-platform-series-oci-streaming.html) (stage 1). That last post got live TfL bus-arrival predictions flowing into the `tfl-arrivals` OCI Streaming topic. This post covers stage 2: a Spark Structured Streaming job that reads the topic, parses it, and lands it as an append-only Delta table, the bronze layer.*

In this post I'll walk through:

- **Reusing what already exists**: this stage needed zero new OCI console resources.
- **Shaping the bronze table**: typed columns plus a full-fidelity `raw_payload` safety net.
- **The Credential Store, not Vault**: a separate, AIDP-native place to keep secrets that a notebook can read directly, and why a preview-feature IAM gap led to one particular choice.
- **Running the stream, and stopping it on purpose**: why the write cell blocks the notebook, the two-step process to stop it, and what to do if a cancelled cell leaves the cluster unresponsive.
- **Verifying it actually worked**: 36,000 rows in a 60-second test run, with the timestamp handling that made that possible.

## Recap: where this data is coming from

Quick reminder of the state of things at the end of the last post: a small Python process on an always-on OCI Compute VM polls the TfL Unified API, deduplicates the near-identical predictions TfL keeps re-serving, and publishes the genuinely new ones onto `tfl-arrivals`, a single-partition OCI Streaming topic inside `tfl-stream-pool`.

<figure>
  <img src="https://zigavaupot.github.io/blogger-ai-data-platform-series/bronze-layer-spark-structured-streaming/images/bronze-source-stream-pool.png" alt="tfl-stream-pool, Active, with the tfl-arrivals stream showing live read/write throughput">
  <figcaption>tfl-stream-pool, Active, with the tfl-arrivals stream showing live read/write throughput.</figcaption>
</figure>

<figure>
  <img src="https://zigavaupot.github.io/blogger-ai-data-platform-series/bronze-layer-spark-structured-streaming/images/bronze-source-stream-messages.png" alt="tfl-arrivals Recent messages tab, showing real offsets and base64-encoded message keys">
  <figcaption>tfl-arrivals Recent messages tab, showing real offsets and base64-encoded message keys.</figcaption>
</figure>

Everything in this post is downstream of that topic. Nothing here talks to the TfL API directly.

## No new OCI resources needed

This stage reuses the `tfl_cluster` compute and the `tfl` catalog's `bronze` schema, both already in place. Everything else happens inside a notebook, `bronze_streaming_job.ipynb`.

Even though the data could have been stored in a dedicated Object Storage bucket per medallion layer, it doesn't need to be: the `bronze` schema already has its own storage, visible in the Master Catalog UI as the `Tables`/`Volumes`/`Knowledge Bases`/`Models` categories underneath it. So the bronze table is created without an explicit `LOCATION` clause: a catalog-managed table sitting on storage the schema already provides, one fewer resource to keep track of.

## Shaping the bronze table

A few decisions made before writing any code:

- **Typed columns for every field in TfL's real payload**, including `timeToLive` and the nested `timing` object. `raw_payload`, the untouched original JSON string, is kept alongside the typed columns as a full-fidelity safety net.
- **One bronze table.** The topic only ever carries arrival-prediction events today, so there's no need to split by entity type.
- **A dedicated checkpoint volume**, `tfl.bronze.tfl_volume`, kept separate from the gold layer's own checkpoint volume, so each layer's pipeline state stays colocated with its own schema instead of borrowing another layer's. That costs one extra `CREATE VOLUME` statement (a catalog-native object, not a new OCI resource); bronze and silver don't follow the same checkpoint convention yet, worth revisiting for silver later.
- **Manual notebook run for now**, the same operating model the stream producer started with on a laptop before it moved to a VM. Automating this into a scheduled Job is a later-stage concern, not a day-one one.

`timing` is stored as a raw `STRING`, parsed straight out of the JSON payload rather than declared as a nested field in the schema.

## Opening the notebook and running the one-time setup

**Create > Notebook** in workspace `workspace001`, named `bronze_streaming_job`, attached to `tfl_cluster`. The notebook mixes `%sql` cells and Python cells, so the default language stays Python: the `%sql` magic handles the SQL cells inline.

<figure>
  <img src="https://zigavaupot.github.io/blogger-ai-data-platform-series/bronze-layer-spark-structured-streaming/images/bronze-create-notebook.png" alt="Workbench Create menu, showing Notebook as an option alongside Job, Python, SQL, and Folder">
  <figcaption>Workbench Create menu.</figcaption>
</figure>

First cell, setting the catalog context:

```sql
%sql
USE CATALOG tfl;
USE SCHEMA bronze;
```

<figure>
  <img src="https://zigavaupot.github.io/blogger-ai-data-platform-series/bronze-layer-spark-structured-streaming/images/bronze-use-catalog-schema.png" alt="Notebook cell running USE CATALOG tfl; USE SCHEMA bronze; returning OK">
  <figcaption>bronze_streaming_job.ipynb, attached to tfl_cluster, running the catalog-context cell.</figcaption>
</figure>

Then the checkpoint volume and the table itself, both idempotent so they're safe to re-run:

```sql
%sql
CREATE VOLUME IF NOT EXISTS tfl.bronze.tfl_volume;
```

<figure>
  <img src="https://zigavaupot.github.io/blogger-ai-data-platform-series/bronze-layer-spark-structured-streaming/images/bronze-create-volume-sql.png" alt="CREATE VOLUME IF NOT EXISTS tfl.bronze.tfl_volume cell, returning status CREATED">
  <figcaption>Volume created, status CREATED.</figcaption>
</figure>

```sql
%sql
CREATE TABLE IF NOT EXISTS tfl.bronze.arrivals_bronze (
  id                      STRING,
  operationType           INT,
  vehicleId               STRING,
  naptanId                STRING,
  stationName             STRING,
  lineId                  STRING,
  lineName                STRING,
  platformName            STRING,
  direction               STRING,
  bearing                 STRING,
  tripId                  STRING,
  baseVersion             STRING,
  destinationNaptanId     STRING,
  destinationName         STRING,
  timestamp               TIMESTAMP,
  timeToStation           INT,
  currentLocation         STRING,
  towards                 STRING,
  expectedArrival         TIMESTAMP,
  timeToLive              TIMESTAMP,
  modeName                STRING,
  timing                  STRING,
  ingest_ts               TIMESTAMP,
  ingest_source           STRING,
  raw_payload             STRING,
  event_date              DATE,
  event_hour              INT
)
USING DELTA
PARTITIONED BY (event_date, event_hour)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'  = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg'
);
```

<figure>
  <img src="https://zigavaupot.github.io/blogger-ai-data-platform-series/bronze-layer-spark-structured-streaming/images/bronze-create-table-sql.png" alt="CREATE TABLE IF NOT EXISTS tfl.bronze.arrivals_bronze DDL cell, executed successfully">
  <figcaption>Bronze table created.</figcaption>
</figure>

Worth verifying directly in the Master Catalog tree, not just the cell output:

<figure>
  <img src="https://zigavaupot.github.io/blogger-ai-data-platform-series/bronze-layer-spark-structured-streaming/images/bronze-volume-created.png" alt="Master Catalog tree showing tfl.bronze.tfl_volume under Volumes">
  <figcaption>tfl.bronze.tfl_volume under Volumes.</figcaption>
</figure>

**Run both statements, in order.** Pasting the `CREATE TABLE` alone without first running `CREATE VOLUME` produces a `VolumePathDoesNotExistException` once you get to the write-stream step later on, an error that surfaces well after the fact and isn't obviously connected back to this step.

## The Credential Store, not Vault

`aidputils.secrets.get(name=..., key=...)`, the call the credentials cell uses, does **not** read OCI Vault secrets directly. It reads from AIDP's own **Credential Store** (Workbench sidebar, currently a Preview feature), which is a separate concept from Vault's Secrets Management entirely.

<figure>
  <img src="https://zigavaupot.github.io/blogger-ai-data-platform-series/bronze-layer-spark-structured-streaming/images/bronze-volume-table-created.png" alt="Master Catalog tree showing tfl.bronze.arrivals_bronze under Tables">
  <figcaption>tfl.bronze.arrivals_bronze under Tables.</figcaption>
</figure>

Before running the credentials cell, the credential it expects has to exist:

1. **Credential store > Create.** Name: `tfl_kafka`.
2. Credential type: **Secret token**, not "Vault reference." A Vault reference (pointing at a Vault secret OCID instead of storing the value directly) is the cleaner option in principle, but creating one failed here with an IAM error: the AIDP resource principal isn't authorized to update secret tags in this tenancy. Fixing that means widening tenancy IAM policy for a Preview feature, which isn't worth it for a demo pipeline, so Secret token is used instead.
3. Two Key/Value rows: `KAFKA_USERNAME` and `KAFKA_PASSWORD`, holding the same values already stored in the `tfl-kafka-usr`/`tfl-kafka-pwd` Vault secrets from the producer setup. **Double-check these two aren't swapped** between the Key/Value rows: mixing them up produces a `SaslAuthenticationException: Authentication failed` that gives no hint the values are just in the wrong slots.

<figure>
  <img src="https://zigavaupot.github.io/blogger-ai-data-platform-series/bronze-layer-spark-structured-streaming/images/bronze-credential-store-list.png" alt="Credential Store list showing tfl_kafka after creation">
  <figcaption>tfl_kafka listed in the Credential Store.</figcaption>
</figure>

With that in place, the credentials cell:

```python
KAFKA_BOOTSTRAP_SERVERS = "cell-1.streaming.eu-frankfurt-1.oci.oraclecloud.com:9092"
KAFKA_TOPIC = "tfl-arrivals"

KAFKA_USERNAME = aidputils.secrets.get(name="tfl_kafka", key="KAFKA_USERNAME")
KAFKA_PASSWORD = aidputils.secrets.get(name="tfl_kafka", key="KAFKA_PASSWORD")
```

then the schema:

```python
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType
)
from pyspark.sql.functions import (
    col, from_json, get_json_object, current_timestamp, lit, to_date, hour
)

tfl_schema = StructType([
    StructField("id", StringType()),
    StructField("operationType", IntegerType()),
    StructField("vehicleId", StringType()),
    StructField("naptanId", StringType()),
    StructField("stationName", StringType()),
    StructField("lineId", StringType()),
    StructField("lineName", StringType()),
    StructField("platformName", StringType()),
    StructField("direction", StringType()),
    StructField("bearing", StringType()),
    StructField("tripId", StringType()),
    StructField("baseVersion", StringType()),
    StructField("destinationNaptanId", StringType()),
    StructField("destinationName", StringType()),
    StructField("timestamp", StringType()),          # parsed -> TIMESTAMP below
    StructField("timeToStation", IntegerType()),
    StructField("currentLocation", StringType()),
    StructField("towards", StringType()),
    StructField("expectedArrival", StringType()),    # parsed -> TIMESTAMP below
    StructField("timeToLive", StringType()),         # parsed -> TIMESTAMP below
    StructField("modeName", StringType()),
])
```

the Kafka `readStream`:

```python
MAX_OFFSETS_PER_TRIGGER = "50000"  # throttle if a micro-batch gets too big

raw_kafka_df = (
    spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option(
            "kafka.sasl.jaas.config",
            'org.apache.kafka.common.security.plain.PlainLoginModule required '
            f'username="{KAFKA_USERNAME}" password="{KAFKA_PASSWORD}";'
        )
        .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
        .load()
)
```

and the parse/transform into `bronze_df`:

```python
bronze_df = (
    raw_kafka_df
        .select(col("value").cast("string").alias("raw_payload"))
        .withColumn("json", from_json(col("raw_payload"), tfl_schema))
        .select("raw_payload", "json.*")
        .withColumn("timing", get_json_object(col("raw_payload"), "$.timing"))
        .withColumn("timestamp", col("timestamp").cast("timestamp"))
        .withColumn("expectedArrival", col("expectedArrival").cast("timestamp"))
        .withColumn("timeToLive", col("timeToLive").cast("timestamp"))
        # Bronze metadata
        .withColumn("ingest_ts", current_timestamp())
        .withColumn("ingest_source", lit(f"oci_stream:{KAFKA_TOPIC}"))
        # Partitions derived from event time
        .withColumn("event_date", to_date(col("timestamp")))
        .withColumn("event_hour", hour(col("timestamp")))
)
```

One thing worth knowing if a credential value ever changes and needs a re-run: the `readStream` cell bakes the current `KAFKA_USERNAME`/`KAFKA_PASSWORD` into its SASL config as literal text the moment that cell executes; Spark doesn't re-read the Python variables later. After fixing a credential, both the credentials cell *and* the `readStream` cell (and the parse cell, which depends on its output) need to be re-run before retrying the write stream, not just the credentials cell alone.

## Running the stream, and stopping it on purpose

```python
CHECKPOINT_PATH = "/Volumes/tfl/bronze/tfl_volume/checkpoints/arrivals-bronze"

query = (
    bronze_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="2 seconds")
        .toTable("tfl.bronze.arrivals_bronze")
)

#######
# this part is for testing only;
#######
#test_query.awaitTermination(timeout=60)    # seconds
#test_query.stop()

query.awaitTermination()
```

<figure>
  <img src="https://zigavaupot.github.io/blogger-ai-data-platform-series/bronze-layer-spark-structured-streaming/images/bronze-streaming-running.png" alt="The streaming write cell, running indefinitely against the bronze Delta table with a 2-second trigger">
  <figcaption>The write-stream cell, running indefinitely.</figcaption>
</figure>

Run it whenever data should be flowing. It runs indefinitely and on demand, matching a manual-run model, no scheduled Job. It also **blocks the notebook**: the cell keeps running until stopped, and no other cell in that same notebook can execute while it's active. Checking on the table's contents while the stream is running needs a second notebook attached to the same cluster: its kernel session is independent, so it can safely run read-only `%sql` queries against `tfl.bronze.arrivals_bronze` concurrently. The commented-out lines above show a bounded test run instead, useful the first time through, before switching over to the indefinite version once the wiring is confirmed.

Stopping it is two steps, not one, because the write cell blocks the kernel:

1. **Interrupt the running cell**, the stop button, or Kernel > Interrupt. This raises a `KeyboardInterrupt` inside `awaitTermination()` and frees the kernel again, but the streaming query itself keeps running in the background until told to stop.
2. **Run the next cell**, `query.stop()`, to actually stop the query and release the checkpoint.

```python
query.stop()
```

```python
print(query.isActive)  # expect: False
```

To check whether a stream is already active before interrupting, useful when picking the notebook back up unsure if a previous run is still going, the notebook's own Spark UI Streaming tab is the way to check.

One thing worth knowing: cancelling a blocked cell from the console's own Cancel button, rather than letting it interrupt and `query.stop()` normally, can leave the notebook's cluster session unresponsive afterward, even a trivial `1+1` failing to run. If that happens, detach the notebook from its cluster and reattach it: that resets the session without restarting the cluster or affecting the checkpoint.

## Verifying it worked

Two `%sql` cells, run from a second notebook while the stream is running, or from the same one once it's stopped:

```sql
%sql
SELECT count(*) AS row_count, max(ingest_ts) AS last_ingest
FROM tfl.bronze.arrivals_bronze;
```

<figure>
  <img src="https://zigavaupot.github.io/blogger-ai-data-platform-series/bronze-layer-spark-structured-streaming/images/bronze-verify-query.png" alt="Verification query result: row_count 36000, last_ingest 2026-09-13 08:49:39.517Z">
  <figcaption>36,000 rows landed during the test run.</figcaption>
</figure>

```sql
%sql
SELECT id, vehicleId, lineId, stationName, timestamp, expectedArrival, timeToLive, ingest_ts
FROM tfl.bronze.arrivals_bronze
ORDER BY ingest_ts DESC
LIMIT 20;
```

<figure>
  <img src="https://zigavaupot.github.io/blogger-ai-data-platform-series/bronze-layer-spark-structured-streaming/images/bronze-verify-sample.png" alt="Sample of the 20 most recently ingested rows, with real station names and timestamps">
  <figcaption>Sample of the 20 most recently ingested rows.</figcaption>
</figure>

A 60-second bounded test run landed 36,000 rows with no null timestamps across `timestamp`, `expectedArrival`, and `timeToLive`: despite `timestamp`'s extra fractional-second digit in the raw payload, the plain `.cast("timestamp")` in the parse cell handles it fine, no format string needed.

This step works whether the stream is still running or has just been stopped: the table keeps whatever rows already landed either way.

## What's next

With `tfl.bronze.arrivals_bronze` filling up as an append-only, full-fidelity landing zone, the next post covers the **silver layer**: cleaning and deduplicating this data with a `MERGE` upsert, and starting to reconcile the raw JSON-shaped bronze columns into something more query-friendly downstream.

*Related: [Just Streams: Real-Time Data Pipelines on OCI](https://zigavaupot.blogspot.com/2026/08/ai-data-platform-series-just-streams.html) (series intro), [Setting Up the AI Data Platform Environment](https://zigavaupot.blogspot.com/2026/09/ai-data-platform-series-setting-up-ai.html), [OCI Streaming and the Stream Producer](https://zigavaupot.blogspot.com/2026/09/ai-data-platform-series-oci-streaming.html)*
