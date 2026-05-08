#!/usr/bin/env bash
# start_pipeline.sh
# Run this INSIDE the cs523bdt-lab container to bring up the full pipeline.
# Usage:  docker exec cs523bdt-lab bash /opt/my_code/cs523-flight-pipeline/start_pipeline.sh

set -e
PROJECT=/opt/my_code/cs523-flight-pipeline
KAFKA_BROKER=172.18.0.4:9092

echo "=== [0/6] Installing Python dependencies ==="
curl -sS https://bootstrap.pypa.io/get-pip.py | python3 - --break-system-packages -q
pip install kafka-python requests python-dotenv happybase --break-system-packages -q
echo "  Done."

echo "=== [1/6] Starting HBase ==="
# Always clear stale /hbase ZooKeeper node — prevents 'Master is initializing' hangs
# after container recreates (stale ephemeral nodes from the previous run)
stop-hbase.sh 2>&1 | tail -1 || true
sleep 3
echo "  Clearing stale /hbase ZooKeeper node..."
echo 'rmr /hbase' | hbase zkcli 2>&1 | grep -E 'CONNECTED|rmr' || true
sleep 2
start-hbase.sh 2>&1 | grep -E 'running|starting' || true

echo "  Waiting for HBase Master to initialize..."
for i in $(seq 1 30); do
  sleep 5
  STATUS=$(echo 'status' | hbase shell 2>/dev/null | grep -E '^\d+ (active|servers)')
  if [ -n "$STATUS" ]; then echo "  HBase ready: $STATUS"; break; fi
  echo "  ...waiting ($((i*5))s)"
done

echo "=== [2/6] Starting HBase Thrift ==="
hbase-daemon.sh start thrift 2>&1 | grep running || true
sleep 3

echo "=== [3/6] Ensuring HBase tables exist ==="
echo "
create 'flights_live', {NAME=>'info',VERSIONS=>1},{NAME=>'pos',VERSIONS=>1},{NAME=>'meta',VERSIONS=>1},{NAME=>'enrich',VERSIONS=>1}
create 'flights_agg', {NAME=>'m',VERSIONS=>1}
create 'flights_alerts', {NAME=>'a',VERSIONS=>1}
create 'flights_enriched', {NAME=>'raw',VERSIONS=>1},{NAME=>'enrich',VERSIONS=>1}
list
exit
" | hbase shell 2>&1 | grep -E 'Created|already exists|flights_|ERROR' || true

echo "=== [4/6] Ensuring HDFS dirs and aircraft.csv exist ==="
hadoop fs -mkdir -p /user/static /user/spark/checkpoints 2>/dev/null || true
if ! hadoop fs -test -e /user/static/aircraft.csv 2>/dev/null; then
  echo "  Downloading aircraft.csv (~90MB)..."
  wget -q -O /tmp/aircraft.csv \
    "https://opensky-network.org/datasets/metadata/aircraftDatabase.csv"
  hadoop fs -put /tmp/aircraft.csv /user/static/aircraft.csv
fi
echo "  aircraft.csv: $(hadoop fs -du -h /user/static/aircraft.csv 2>/dev/null | awk '{print $1}')"

echo "=== [5/6] Starting Kafka producer ==="
pkill -f producer.py 2>/dev/null || true
sleep 1
PYTHONUNBUFFERED=1 KAFKA_BOOTSTRAP_SERVERS=${KAFKA_BROKER} \
  nohup python3 -u ${PROJECT}/producer/producer.py \
  > /tmp/producer.log 2>&1 &
echo "  Producer PID: $!"
sleep 5
tail -3 /tmp/producer.log

echo "=== [6/6] Starting Spark Structured Streaming ==="
pkill -f stream_processor 2>/dev/null || true
sleep 2
PYTHONUNBUFFERED=1 HBASE_HOST=localhost KAFKA_BOOTSTRAP_SERVERS=${KAFKA_BROKER} \
  nohup spark-submit \
    --master local[2] \
    --driver-memory 2g \
    --conf spark.driver.maxResultSize=512m \
    --conf spark.sql.shuffle.partitions=2 \
    --conf spark.streaming.backpressure.enabled=true \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 \
    ${PROJECT}/spark_app/stream_processor.py \
  > /tmp/spark.log 2>&1 &
echo "  Spark PID: $!"
echo "  Tailing spark.log — waiting for queries to start (up to 90s)..."
for i in $(seq 1 18); do
  sleep 5
  if grep -q "All 4 queries started" /tmp/spark.log 2>/dev/null; then
    echo "  Spark streaming queries confirmed running."
    break
  fi
  if grep -q "ERROR\|Exception" /tmp/spark.log 2>/dev/null; then
    echo "  ERROR detected in spark.log:"; grep "ERROR\|Exception" /tmp/spark.log | head -5; break
  fi
  echo "  ...waiting ($((i*5))s)"
done

echo "=== Starting Streamlit dashboard ==="
pip install streamlit --break-system-packages -q 2>/dev/null || true
pkill -f "streamlit run" 2>/dev/null || true
sleep 1
HBASE_HOST=localhost nohup streamlit run ${PROJECT}/dashboard/app.py \
  --server.port 8501 \
  --server.address 0.0.0.0 \
  --server.headless true \
  > /tmp/dashboard.log 2>&1 &
echo "  Dashboard PID: $!"
sleep 5
grep -E 'started|URL|Error' /tmp/dashboard.log | head -4

echo ""
echo "=== Pipeline status ==="
echo "  Producer log:  docker exec cs523bdt-lab tail -5 /tmp/producer.log"
echo "  Spark log:     docker exec cs523bdt-lab tail -20 /tmp/spark.log"
echo "  Dashboard:     http://localhost:8501"
echo "  HBase verify:  docker exec cs523bdt-lab python3 -c \""
echo "    import happybase; c=happybase.Connection('localhost')"
echo "    [print(t, len(list(c.table(t).scan(limit=1)))) for t in ['flights_live','flights_agg']]\""
