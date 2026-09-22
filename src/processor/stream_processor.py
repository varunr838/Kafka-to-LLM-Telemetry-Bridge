import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, avg, count, when, window
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType
)

# ─────────────────────────────────────────────
# 1. SPARK SESSION
# ─────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("KafkaToLLMTelemetryBridge")
    # Kafka connector
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
        "io.delta:delta-spark_2.12:3.1.0",
    )
    # Delta Lake configuration
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    # Default source format
    .config("spark.sql.sources.default", "delta")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
print("✅ SparkSession initialised.")

# ─────────────────────────────────────────────
# 2. SCHEMA DEFINITION
#    We define the FULL evolved schema (with the
#    optional user_tier field).  Spark leaves
#    user_tier as NULL for pre-evolution records —
#    this is how we handle schema evolution.
# ─────────────────────────────────────────────
telemetry_schema = StructType([
    StructField("log_id",           StringType(),  True),
    StructField("user_id",          StringType(),  True),
    StructField("timestamp",        StringType(),  True),   # parsed to Timestamp below
    StructField("endpoint",         StringType(),  True),
    StructField("status_code",      IntegerType(), True),
    StructField("response_time_ms", LongType(),    True),
    StructField("user_tier",        StringType(),  True),   # Schema-evolution field (nullable)
])

# ─────────────────────────────────────────────
# 3. READ STREAM FROM KAFKA
# ─────────────────────────────────────────────
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
TOPIC        = os.getenv("KAFKA_TOPIC",  "telemetry_logs")

raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

# Cast binary Kafka value → string → parsed JSON struct
parsed_stream = raw_stream.select(
    from_json(col("value").cast("string"), telemetry_schema).alias("data")
).select("data.*")

print("✅ Kafka stream connected and schema applied.")

# ─────────────────────────────────────────────
# 4. TIMESTAMP CASTING
# ─────────────────────────────────────────────
typed_stream = parsed_stream.withColumn(
    "event_time",
    to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'")
)

# ─────────────────────────────────────────────
# 5. foreachBatch: GOLD + DLQ WRITER
#    Structured Streaming does not natively allow
#    writing one stream to two sinks simultaneously.
#    foreachBatch receives each micro-batch as a
#    plain static DataFrame, so we can freely split
#    and write it anywhere.
# ─────────────────────────────────────────────
GOLD_PATH = os.getenv("GOLD_PATH", "data/gold_logs")
DLQ_PATH  = os.getenv("DLQ_PATH",  "data/dlq_logs")

CHECKPOINT_GOLD = "data/_checkpoints/gold"
CHECKPOINT_DLQ  = "data/_checkpoints/dlq"

def process_batch(batch_df, batch_id):
    """
    Called once per micro-batch.
    Splits the batch into good and bad records and
    writes each to its own Delta table.
    """
    if batch_df.isEmpty():
        print(f"[Batch {batch_id}] Empty batch, skipping.")
        return

    # ── Good records (Gold) ──────────────────────────────
    # Valid user_id AND status_code within the normal range
    gold_df = batch_df.filter(
        col("user_id").isNotNull()
        & (col("status_code") <= 599)
    )

    # ── Bad records (DLQ) ────────────────────────────────
    # Missing user_id OR out-of-range status code
    dlq_df = batch_df.filter(
        col("user_id").isNull()
        | (col("status_code") > 599)
    )

    gold_count = gold_df.count()
    dlq_count  = dlq_df.count()
    print(f"[Batch {batch_id}] ✅ Gold: {gold_count} records | ❌ DLQ: {dlq_count} records")

    # Write Gold → Delta, mergeSchema=true handles schema evolution
    # (e.g. when the generator adds 'user_tier' after record 100)
    if gold_count > 0:
        (
            gold_df.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")   # ← schema evolution support
            .save(GOLD_PATH)
        )

    # Write DLQ → Delta (no mergeSchema needed; we keep the raw shape)
    if dlq_count > 0:
        (
            dlq_df.write
            .format("delta")
            .mode("append")
            .save(DLQ_PATH)
        )

# ─────────────────────────────────────────────
# 6. WATERMARK + TUMBLING WINDOW AGGREGATION
#    Operates on the typed_stream directly so it
#    runs as its own independent streaming query.
# ─────────────────────────────────────────────
windowed_agg = (
    typed_stream
    .filter(                                         # only aggregate clean records
        col("user_id").isNotNull()
        & col("event_time").isNotNull()
        & (col("status_code") <= 599)
    )
    .withWatermark("event_time", "5 minutes")        # tolerate up to 5-min late arrivals
    .groupBy(
        window(col("event_time"), "1 minute"),        # 1-minute tumbling window
        col("user_id"),
    )
    .agg(
        avg("response_time_ms").alias("avg_response_time_ms"),
        count(when(col("status_code") >= 500, True)).alias("error_5xx_count"),
        count("*").alias("total_requests"),
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("user_id"),
        col("avg_response_time_ms"),
        col("error_5xx_count"),
        col("total_requests"),
    )
)

# ─────────────────────────────────────────────
# 7. SINK DEFINITIONS
# ─────────────────────────────────────────────
AGG_PATH        = os.getenv("AGG_PATH", "data/agg_per_user_window")
CHECKPOINT_AGG  = "data/_checkpoints/agg"

# ── 7a. Gold + DLQ via foreachBatch ──────────────────────
#    A single streaming query drives both writes.
#    The split happens inside process_batch().
query_gold_dlq = (
    typed_stream.writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", CHECKPOINT_GOLD)
    .start()
)
print(f"✅ Gold/DLQ foreachBatch query started (gold→{GOLD_PATH}, dlq→{DLQ_PATH})")

# ── 7b. Windowed aggregation → Delta ─────────────────────
query_agg = (
    windowed_agg.writeStream
    .format("delta")
    .outputMode("append")           # 'append' is valid with watermark
    .option("checkpointLocation", CHECKPOINT_AGG)
    .start(AGG_PATH)
)
print(f"✅ Aggregation stream writing to: {AGG_PATH}")

# ─────────────────────────────────────────────
# 8. AWAIT TERMINATION
# ─────────────────────────────────────────────
print("🚀 All streaming queries running. Awaiting termination…")
spark.streams.awaitAnyTermination()
