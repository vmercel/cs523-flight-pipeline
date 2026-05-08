# CS523 — Real-Time Flight Analytics Pipeline

**Authors:** Alvin Leonald Kabwama · Mercel Vubangsi
**Course:** CS523 Big Data Technology — Maharishi International University
**Stack:** OpenSky API → Apache Kafka → PySpark Structured Streaming (YARN) → HBase → Streamlit

---

## Architecture

```
OpenSky REST API (poll every 10 s)
        │
        ▼
Python Producer  ──►  Kafka topic: flights-raw  (3 partitions)
                                │
                                ▼
             PySpark Structured Streaming on YARN
             ├─ Enrich: broadcast join with aircraft.csv (HDFS)
             ├─ Q1  Latest snapshot          → HBase flights_live
             ├─ Q2  5-min sliding window agg → HBase flights_agg
             ├─ Q3  Rapid-descent alerts     → HBase flights_alerts
             └─ Q4  Full enriched archive    → HBase flights_enriched
                                │
                        HBase Thrift :9090
                                │
                                ▼
                     Streamlit Dashboard
                     ├─ Tab 1: Live world map
                     ├─ Tab 2: Country trend charts
                     └─ Tab 3: Anomaly alert feed
```

## Project Rubric Coverage

| Part | Points | Deliverable |
|------|--------|-------------|
| 1 — Kafka ingestion | 3 | `producer/producer.py` |
| 2 — Spark Structured Streaming | 3 | `spark_app/stream_processor.py` (Q1–Q3) |
| 3 — HBase persistence | 2 | Four HBase tables |
| 4 — Visualisation dashboard | 2 | `dashboard/app.py` (Streamlit) |
| 5 — Spark SQL static join (bonus) | +2 | Integrated in `stream_processor.py` (Q4) |
| **Total** | **10 + 2** | |

---

## Prerequisites

- Docker Desktop running with the `cs523-bdt` compose stack (`docker-compose up -d`)
- Python 3.x on the host for the dashboard (`pip install -r dashboard/requirements.txt`)

---

## One-Time Setup (inside the container)

```bash
docker exec -it cs523bdt-lab bash
```

### 1. Create the Kafka topic

```bash
kafka-topics.sh --create \
  --topic flights-raw \
  --partitions 3 \
  --replication-factor 1 \
  --bootstrap-server localhost:9092

# Verify
kafka-topics.sh --list --bootstrap-server localhost:9092
```

### 2. Start HBase and create tables

```bash
# Start HBase daemons
start-hbase.sh

# Start Thrift server (required by dashboard and Spark writer)
hbase-daemon.sh start thrift

# Create tables
hbase shell < /opt/my_code/cs523-flight-pipeline/hbase/schema.hbase
```

### 3. Load static aircraft metadata to HDFS

```bash
# Download the OpenSky aircraft metadata CSV (~50 MB)
wget -O /opt/my_code/cs523-flight-pipeline/data/static/aircraft.csv \
  "https://opensky-network.org/datasets/metadata/aircraftDatabase.csv"

# Upload to HDFS
hadoop fs -mkdir -p /user/static
hadoop fs -put /opt/my_code/cs523-flight-pipeline/data/static/aircraft.csv /user/static/

# Verify
hadoop fs -ls /user/static/
```

### 4. Create Spark checkpoint directories

```bash
hadoop fs -mkdir -p /user/spark/checkpoints
```

---

## Running the Pipeline

### Step 1 — Start the Kafka producer (host machine or container)

```bash
cd producer
pip install -r requirements.txt
python producer.py
```

Watch for output like:
```
[producer] poll=1  sent=6043 messages  ts=1746576010
```

### Step 2 — Submit the Spark streaming job (inside container)

```bash
spark-submit \
  --master local[2] \
  --driver-memory 1g \
  --conf spark.sql.shuffle.partitions=2 \
  --conf spark.streaming.backpressure.enabled=true \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 \
  /opt/my_code/cs523-flight-pipeline/spark_app/stream_processor.py
```

> **Note on `local[2]` vs `yarn`:** The pseudo-distributed single-node container
> runs NameNode, DataNode, ResourceManager, NodeManager, HMaster, HRegionServer,
> and Kafka all in one container. Using `--master yarn` causes YARN to launch
> competing JVM containers that exhaust the available memory (~7.6 GiB).
> `--master local[2]` runs the Spark driver + 2 threads in a single JVM and
> leaves enough headroom for all other services. All Structured Streaming
> features (watermarks, windowed aggregations, checkpointing, foreachBatch) work
> identically in local mode — this satisfies the rubric requirement for
> Spark Structured Streaming.

### Step 3 — Start the Streamlit dashboard (host machine)

```bash
cd dashboard
pip install -r requirements.txt
HBASE_HOST=localhost streamlit run app.py
```

Open http://localhost:8501 in your browser.

---

## Verify Data in HBase

```bash
# Inside container
hbase shell

# Latest aircraft states
scan 'flights_live', {LIMIT => 5}

# Windowed aggregations
scan 'flights_agg', {LIMIT => 10}

# Anomaly alerts
scan 'flights_alerts', {LIMIT => 10}

# Enriched archive (bonus)
scan 'flights_enriched', {LIMIT => 5}
```

---

## Fallback: OpenSky Offline

If OpenSky is unreachable, the producer automatically loads
`data/static/replay.json` (a pre-recorded snapshot). To create it:

```bash
curl "https://opensky-network.org/api/states/all" | \
  python3 -c "import sys,json; d=json.load(sys.stdin); json.dump(d['states'],open('data/static/replay.json','w'))"
```

---

## Stopping Everything

```bash
# Host
docker-compose down        # stop containers (keeps volumes)
docker-compose down -v     # also wipe HBase / Kafka data

# Inside container — stop HBase gracefully
stop-hbase.sh
```

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `HBASE_HOST` | `localhost` | HBase Thrift server hostname |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka-server:9092` | Kafka broker address |
| `OPENSKY_USERNAME` | _(empty)_ | OpenSky account (optional, raises rate limit) |
| `OPENSKY_PASSWORD` | _(empty)_ | OpenSky password |

Copy `.env.example` to `.env` and fill in values as needed.
