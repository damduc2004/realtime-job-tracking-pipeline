import logging
from datetime import datetime
import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


# ── Logging ───────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ── Config ────────────────────────────────────────────────────────
KAFKA_BROKER       = "localhost:9092"
KAFKA_TOPIC_INPUT  = "tracking-events"

MYSQL_HOST         = "localhost"
MYSQL_PORT         = "3306"
MYSQL_DATABASE     = "logs"
MYSQL_USER         = "root"
MYSQL_PASSWORD     = "1"
MYSQL_DRIVER       = "com.mysql.cj.jdbc.Driver"
MYSQL_URL          = f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"
MYSQL_EVENTS_TABLE = "events"

TRIGGER_SECONDS    = 10            # flush mỗi 10 giây
WATERMARK_DELAY    = "10 minutes"  # chấp nhận event đến trễ tối đa 10 phút


# ── Schema raw event (khớp với kafka_producer.py) ─────────────────
RAW_EVENT_SCHEMA = StructType([
    StructField("create_time",  StringType(),  True),
    StructField("bid",          IntegerType(), True),
    StructField("campaign_id",  IntegerType(), True),
    StructField("custom_track", StringType(),  True),
    StructField("group_id",     IntegerType(), True),
    StructField("job_id",       IntegerType(), True),
    StructField("publisher_id", IntegerType(), True),
    StructField("ts",           StringType(),  True),
])


# ── SparkSession ──────────────────────────────────────────────────
def build_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("Kafka_Streaming_Consumer")
        .config(
            "spark.jars.packages",
            ",".join([
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3",
                "mysql:mysql-connector-java:8.0.33",
            ]),
        )
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


# ── Build streaming pipeline ──────────────────────────────────────
def build_aggregated_stream(spark: SparkSession):
    # ── Extract: đọc raw bytes từ Kafka ──────────────────────────
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_TOPIC_INPUT)
        .option("startingOffsets", "latest")   # chỉ xử lý event mới
        .option("failOnDataLoss", "false")
        .load()
    )

    # ── Transform bước 1: parse JSON ─────────────────────────────
    events = (
        raw
        .selectExpr("CAST(value AS STRING) AS json_str")
        .withColumn("e", from_json(col("json_str"), RAW_EVENT_SCHEMA))
        .select("e.*")
        .withColumn("event_time", to_timestamp(col("ts"), "yyyy-MM-dd HH:mm:ss"))
        .drop("ts")
    )

    # ── Transform bước 2: watermark + group + aggregate ──────────
    return (
        events
        .withWatermark("event_time", WATERMARK_DELAY)
        .groupBy(
            to_date(col("event_time")).alias("dates"),
            hour(col("event_time")).alias("hours"),
            col("job_id"),
            col("campaign_id"),
            col("group_id"),
            col("publisher_id"),
        )
        .agg(
            count(
                when(col("custom_track") == "click", 1)
            ).alias("clicks"),

            round(
                avg(when(col("custom_track") == "click", col("bid").cast("double"))), 2
            ).alias("bid_set"),

            round(
                sum(when(col("custom_track") == "click", col("bid").cast("double"))), 2
            ).alias("spend_hour"),

            count(
                when(col("custom_track") == "conversion", 1)
            ).alias("conversion"),

            count(
                when(col("custom_track") == "qualified", 1)
            ).alias("qualified_application"),

            count(
                when(col("custom_track") == "unqualified", 1)
            ).alias("disqualified_application"),
        )
        .withColumn("sources", lit("Kafka-Streaming"))
    )


# ── foreachBatch writer ───────────────────────────────────────────
def make_batch_writer(spark: SparkSession):
    def write_batch_to_mysql(batch_df, batch_id):
        n      = batch_df.count()
        ts_now = datetime.now().strftime("%H:%M:%S")

        if n == 0:
            log.info(f"[{ts_now}] Batch {batch_id:>4} | empty – skip")
            return
        job_dim = (
            spark.read.format("jdbc")
            .options(
                url      = MYSQL_URL,
                driver   = MYSQL_DRIVER,
                dbtable  = "(SELECT id AS job_id, company_id FROM job) A",
                user     = MYSQL_USER,
                password = MYSQL_PASSWORD,
            )
            .load()
        )

        enriched = batch_df.join(broadcast(job_dim), on="job_id", how="left")

        output = enriched.select(
            col("job_id"),
            col("dates"),
            col("hours"),
            col("disqualified_application"),
            col("qualified_application"),
            col("conversion"),
            col("company_id"),
            col("group_id"),
            col("campaign_id"),
            col("publisher_id"),
            col("bid_set"),
            col("clicks"),
            col("spend_hour"),
            col("sources"),
            lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")).alias("updated_at"),
        )

        (
            output.write.format("jdbc")
            .option("url",      MYSQL_URL)
            .option("driver",   MYSQL_DRIVER)
            .option("dbtable",  MYSQL_EVENTS_TABLE)
            .option("user",     MYSQL_USER)
            .option("password", MYSQL_PASSWORD)
            .mode("append")
            .save()
        )

        log.info(f"[{ts_now}] Batch {batch_id:>4} | wrote {n:>4} rows → MySQL {MYSQL_EVENTS_TABLE}")

    return write_batch_to_mysql


# ── Main ──────────────────────────────────────────────────────────
def main():
    spark        = build_spark_session()
    aggregated   = build_aggregated_stream(spark)
    batch_writer = make_batch_writer(spark)

    query = (
        aggregated.writeStream
        .outputMode("update")                              
        .foreachBatch(batch_writer)
        .trigger(processingTime=f"{TRIGGER_SECONDS} seconds")
        .start()
    )

    log.info("=" * 55)
    log.info(f"Spark {spark.version} | Streaming started")
    log.info(f"  Input   : Kafka topic '{KAFKA_TOPIC_INPUT}'")
    log.info(f"  Output  : MySQL '{MYSQL_EVENTS_TABLE}'")
    log.info(f"  Watermark : {WATERMARK_DELAY}")
    log.info(f"  Trigger   : every {TRIGGER_SECONDS}s")
    log.info("=" * 55)
    log.info("Waiting for events... (Ctrl+C to stop)")

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        query.stop()
        log.info("Streaming stopped.")


if __name__ == "__main__":
    main()
