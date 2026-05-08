"""
Parts 2, 3, and 5 (bonus) — Distributed Processing, Persistent Storage, Spark SQL Join
Single PySpark Structured Streaming application that:
  - Reads from Kafka topic 'flights-raw'
  - Enriches the stream via a Spark SQL broadcast join with the static
    aircraft metadata CSV stored on HDFS  (Part 5 bonus)
  - Runs four concurrent streaming queries:
      Q1  Latest snapshot per aircraft     → HBase flights_live
      Q2  5-min sliding / 1-min step agg   → HBase flights_agg
      Q3  Rapid-descent anomaly filter     → HBase flights_alerts
      Q4  Full enriched record archive     → HBase flights_enriched
"""

import os

import happybase
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    broadcast,
    col,
    count,
    from_json,
    max as spark_max,
    window,
)
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "172.18.0.4:9092")
TOPIC = "flights-raw"
HBASE_HOST = os.getenv("HBASE_HOST", "localhost")
HDFS_AIRCRAFT_CSV = "hdfs:///user/static/aircraft.csv"
CHECKPOINT_BASE = "hdfs:///user/spark/checkpoints"

# ---------------------------------------------------------------------------
# Flight state schema (matches producer output)
# ---------------------------------------------------------------------------
FLIGHT_SCHEMA = StructType([
    StructField("icao24",          StringType(),  True),
    StructField("callsign",        StringType(),  True),
    StructField("origin_country",  StringType(),  True),
    StructField("time_position",   LongType(),    True),
    StructField("last_contact",    LongType(),    True),
    StructField("longitude",       DoubleType(),  True),
    StructField("latitude",        DoubleType(),  True),
    StructField("baro_altitude",   DoubleType(),  True),
    StructField("on_ground",       BooleanType(), True),
    StructField("velocity",        DoubleType(),  True),
    StructField("true_track",      DoubleType(),  True),
    StructField("vertical_rate",   DoubleType(),  True),
    StructField("geo_altitude",    DoubleType(),  True),
    StructField("squawk",          StringType(),  True),
    StructField("ingestion_ts",    LongType(),    True),
])


# ---------------------------------------------------------------------------
# HBase write helpers
# ---------------------------------------------------------------------------

def _hbase_conn():
    return happybase.Connection(HBASE_HOST)


def write_live(batch_df, batch_id):
    """Q1: Upsert latest flight state into flights_live."""
    rows = batch_df.collect()
    if not rows:
        return
    conn = _hbase_conn()
    try:
        table = conn.table("flights_live")
        with table.batch() as b:
            for r in rows:
                key = (r.icao24 or "unknown").encode()
                b.put(key, {
                    b"info:callsign":    (r.callsign or "").encode(),
                    b"info:country":     (r.origin_country or "").encode(),
                    b"pos:lat":          str(r.latitude  or "").encode(),
                    b"pos:lon":          str(r.longitude or "").encode(),
                    b"pos:baro_alt":     str(r.baro_altitude  or "").encode(),
                    b"pos:geo_alt":      str(r.geo_altitude   or "").encode(),
                    b"pos:velocity":     str(r.velocity       or "").encode(),
                    b"pos:track":        str(r.true_track     or "").encode(),
                    b"pos:vrate":        str(r.vertical_rate  or "").encode(),
                    b"meta:on_ground":   str(r.on_ground).encode(),
                    b"meta:last_contact":str(r.last_contact   or "").encode(),
                    b"meta:ingestion_ts":str(r.ingestion_ts   or "").encode(),
                    b"enrich:operator":  (r.operator          or "").encode(),
                    b"enrich:model":     (r.model             or "").encode(),
                    b"enrich:mfr":       (r.manufacturername  or "").encode(),
                    b"enrich:typecode":  (r.typecode          or "").encode(),
                    b"enrich:reg":       (r.registration      or "").encode(),
                })
    finally:
        conn.close()


def write_agg(batch_df, batch_id):
    """Q2: Write sliding-window aggregations into flights_agg."""
    rows = batch_df.collect()
    if not rows:
        return
    conn = _hbase_conn()
    try:
        table = conn.table("flights_agg")
        with table.batch() as b:
            for r in rows:
                country = (r.origin_country or "UNKNOWN").replace("|", "_")
                win_end = int(r.window.end.timestamp())
                reverse_epoch = str(9999999999 - win_end)
                key = f"{country}|{reverse_epoch}".encode()
                b.put(key, {
                    b"m:flight_count": str(r.flight_count).encode(),
                    b"m:avg_alt":      str(round(r.avg_alt or 0, 2)).encode(),
                    b"m:max_alt":      str(round(r.max_alt or 0, 2)).encode(),
                    b"m:avg_velocity": str(round(r.avg_velocity or 0, 2)).encode(),
                    b"m:window_end":   str(win_end).encode(),
                })
    finally:
        conn.close()


def write_alerts(batch_df, batch_id):
    """Q3: Append rapid-descent anomaly events to flights_alerts."""
    rows = batch_df.collect()
    if not rows:
        return
    conn = _hbase_conn()
    try:
        table = conn.table("flights_alerts")
        with table.batch() as b:
            for r in rows:
                icao = (r.icao24 or "unknown")
                ts = r.ingestion_ts or 0
                reverse_ts = str(9999999999 - ts)
                key = f"{icao}|{reverse_ts}".encode()
                b.put(key, {
                    b"a:vrate":    str(r.vertical_rate  or "").encode(),
                    b"a:alt":      str(r.baro_altitude  or "").encode(),
                    b"a:callsign": (r.callsign          or "").encode(),
                    b"a:operator": (r.operator          or "").encode(),
                    b"a:country":  (r.origin_country    or "").encode(),
                    b"a:ts":       str(ts).encode(),
                })
    finally:
        conn.close()


def write_enriched(batch_df, batch_id):
    """Q4 (bonus): Upsert full enriched records into flights_enriched."""
    rows = batch_df.collect()
    if not rows:
        return
    conn = _hbase_conn()
    try:
        table = conn.table("flights_enriched")
        with table.batch() as b:
            for r in rows:
                key = (r.icao24 or "unknown").encode()
                b.put(key, {
                    b"raw:callsign":       (r.callsign          or "").encode(),
                    b"raw:country":        (r.origin_country    or "").encode(),
                    b"raw:lat":            str(r.latitude       or "").encode(),
                    b"raw:lon":            str(r.longitude      or "").encode(),
                    b"raw:baro_alt":       str(r.baro_altitude  or "").encode(),
                    b"raw:velocity":       str(r.velocity       or "").encode(),
                    b"raw:vrate":          str(r.vertical_rate  or "").encode(),
                    b"raw:on_ground":      str(r.on_ground).encode(),
                    b"raw:last_contact":   str(r.last_contact   or "").encode(),
                    b"enrich:operator":    (r.operator          or "").encode(),
                    b"enrich:model":       (r.model             or "").encode(),
                    b"enrich:mfr":         (r.manufacturername  or "").encode(),
                    b"enrich:typecode":    (r.typecode          or "").encode(),
                    b"enrich:reg":         (r.registration      or "").encode(),
                })
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Main streaming application
# ---------------------------------------------------------------------------

def main():
    spark = (SparkSession.builder
             .appName("CS523-FlightPipeline")
             .getOrCreate())

    spark.sparkContext.setLogLevel("WARN")

    # ------------------------------------------------------------------
    # Step 1 — Load static aircraft metadata from HDFS and cache it.
    # broadcast() ensures no shuffle-side join on the streaming DataFrame.
    # This is the Part 5 Spark SQL bonus enrichment.
    # ------------------------------------------------------------------
    aircraft_df = (spark.read
                   .option("header", True)
                   .option("inferSchema", False)
                   .csv(HDFS_AIRCRAFT_CSV)
                   .select(
                       col("icao24"),
                       col("registration"),
                       col("manufacturername"),
                       col("model"),
                       col("typecode"),
                       col("operator"),
                   )
                   .cache())

    print(f"[stream_processor] Aircraft reference rows: {aircraft_df.count()}")

    # ------------------------------------------------------------------
    # Step 2 — Read raw stream from Kafka
    # ------------------------------------------------------------------
    raw_kafka_df = (spark.readStream
                    .format("kafka")
                    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
                    .option("subscribe", TOPIC)
                    .option("startingOffsets", "latest")
                    .option("failOnDataLoss", "false")
                    .load())

    # Parse JSON payload
    parsed_df = (raw_kafka_df
                 .selectExpr("CAST(value AS STRING) AS json_str")
                 .select(from_json(col("json_str"), FLIGHT_SCHEMA).alias("d"))
                 .select("d.*"))

    # Event-time watermark on last_contact (Unix epoch seconds → cast to timestamp)
    watermarked_df = (parsed_df
                      .withColumn("event_time",
                                  (col("last_contact")).cast("timestamp"))
                      .withWatermark("event_time", "2 minutes"))

    # ------------------------------------------------------------------
    # Step 3 — Enrich: left-join with static aircraft reference data
    # (Part 5 bonus — Spark SQL join of stream + HDFS static dataset)
    # ------------------------------------------------------------------
    enriched_df = watermarked_df.join(broadcast(aircraft_df),
                                      on="icao24",
                                      how="left")

    # ------------------------------------------------------------------
    # Q1 — Latest snapshot (upsert mode, key = icao24)
    # ------------------------------------------------------------------
    q1 = (enriched_df.writeStream
          .trigger(processingTime="10 seconds")
          .foreachBatch(write_live)
          .option("checkpointLocation", f"{CHECKPOINT_BASE}/flights_live")
          .start())

    # ------------------------------------------------------------------
    # Q2 — 5-min sliding window, 1-min step: count + avg/max alt + avg vel
    # ------------------------------------------------------------------
    agg_df = (enriched_df
              .filter(col("origin_country").isNotNull())
              .groupBy(
                  window("event_time", "5 minutes", "1 minute"),
                  col("origin_country"),
              )
              .agg(
                  count("*").alias("flight_count"),
                  avg("baro_altitude").alias("avg_alt"),
                  spark_max("baro_altitude").alias("max_alt"),
                  avg("velocity").alias("avg_velocity"),
              ))

    q2 = (agg_df.writeStream
          .trigger(processingTime="10 seconds")
          .outputMode("update")
          .foreachBatch(write_agg)
          .option("checkpointLocation", f"{CHECKPOINT_BASE}/flights_agg")
          .start())

    # ------------------------------------------------------------------
    # Q3 — Rapid-descent anomaly detection
    # Flag airborne flights with vertical_rate < -15 m/s (~-3000 ft/min)
    # ------------------------------------------------------------------
    alerts_df = (enriched_df
                 .filter(
                     (col("on_ground") == False) &
                     col("vertical_rate").isNotNull() &
                     (col("vertical_rate") < -15)
                 ))

    q3 = (alerts_df.writeStream
          .trigger(processingTime="10 seconds")
          .foreachBatch(write_alerts)
          .option("checkpointLocation", f"{CHECKPOINT_BASE}/flights_alerts")
          .start())

    # ------------------------------------------------------------------
    # Q4 — Full enriched archive (bonus output table)
    # ------------------------------------------------------------------
    q4 = (enriched_df.writeStream
          .trigger(processingTime="10 seconds")
          .foreachBatch(write_enriched)
          .option("checkpointLocation", f"{CHECKPOINT_BASE}/flights_enriched")
          .start())

    print("[stream_processor] All 4 queries started. Awaiting termination...")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
