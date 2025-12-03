import os
import requests
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, from_json, to_json, struct, udf, lit, window, 
    avg, count, collect_list, max as spark_max, min as spark_min, concat_ws
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, MapType, TimestampType


RAW_TOPIC = os.getenv("KAFKA_TOPIC_RAW", "calls.raw")
ENRICHED_TOPIC = os.getenv("KAFKA_TOPIC_ENRICHED", "calls.enriched")
SENTIMENT_TOPIC = os.getenv("KAFKA_TOPIC_SENTIMENT", "calls.sentiment")
BROKERS = os.getenv("KAFKA_BROKERS", "kafka:29092")
WINDOW_SECONDS = int(os.getenv("STREAM_WINDOW_SECONDS", "10"))
SENTIMENT_URL = os.getenv("SENTIMENT_URL", "http://sentiment-service:8000")

raw_schema = StructType([
    StructField("call_id", StringType(), True),
    StructField("utterance_id", StringType(), True),
    StructField("utterance_index", IntegerType(), True),
    StructField("timestamp_ms", LongType(), True),
    StructField("event_time", LongType(), True),
    StructField("chunk_id", StringType(), True),
    StructField("speaker_role", StringType(), True),
    StructField("text", StringType(), True),
    StructField("metadata", MapType(StringType(), StringType()), True),
])

def clean_text(txt: str) -> str:
    if not txt:
        return ""
    t = txt.strip().replace("\n", " ")
    return " ".join(t.split())

from pyspark.sql.types import StructType as S, StructField as F, DoubleType
sentiment_schema = S([F("score", DoubleType(), False), F("label", StringType(), False)])

from pyspark.sql.functions import pandas_udf, PandasUDFType
import pandas as pd

@pandas_udf(sentiment_schema, PandasUDFType.SCALAR)
def qwen_sentiment_udf(texts: pd.Series) -> pd.DataFrame:
    """Call sentiment service to analyze sentiment for batch of texts."""
    scores = []
    labels = []
    
    # Convert to list and filter empty texts
    text_list = texts.tolist()
    non_empty_texts = [t for t in text_list if t and t.strip()]
    
    if not non_empty_texts:
        # Return neutral for all empty texts
        return pd.DataFrame({"score": [0.0] * len(text_list), "label": ["NEU"] * len(text_list)})
    
    try:
        # Call sentiment service
        response = requests.post(
            f"{SENTIMENT_URL}/analyze",
            json={"texts": non_empty_texts},
            timeout=30
        )
        response.raise_for_status()
        data = response.json()
        
        # Map results back to original list (handling empty texts)
        result_map = {}
        for i, result in enumerate(data.get("results", [])):
            if i < len(non_empty_texts):
                result_map[non_empty_texts[i]] = (result.get("score", 0.0), result.get("label", "NEU"))
        
        # Build results matching original order
        for t in text_list:
            if t and t.strip() and t in result_map:
                score, label = result_map[t]
                scores.append(float(score))
                labels.append(str(label).upper())
            else:
                scores.append(0.0)
                labels.append("NEU")
    
    except Exception as e:
        # On error, return neutral sentiment for all
        print(f"Error calling sentiment service: {e}")
        scores = [0.0] * len(text_list)
        labels = ["NEU"] * len(text_list)
    
    return pd.DataFrame({"score": scores, "label": labels})

clean_udf = udf(clean_text, StringType())


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("AgentAssistStreaming")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", BROKERS)
        .option("subscribe", RAW_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )
    parsed = df.selectExpr("CAST(key AS STRING) as key", "CAST(value AS STRING) as value") \
        .select(from_json(col("value"), raw_schema).alias("data")) \
        .select("data.*")

    # Convert event_time to timestamp for windowing
    from pyspark.sql.functions import from_unixtime
    parsed_with_ts = parsed.withColumn(
        "event_timestamp",
        (col("event_time") / 1000).cast("timestamp")
    )

    enriched = (
        parsed_with_ts
        .withColumn("clean_text", clean_udf(col("text")))
        .withColumn("sentiment", qwen_sentiment_udf(col("clean_text")))
        .select(
            col("call_id"),
            col("utterance_id"),
            col("utterance_index"),
            col("timestamp_ms"),
            col("event_time"),
            col("event_timestamp"),
            col("chunk_id"),
            col("clean_text").alias("text"),
            col("metadata"),
            col("sentiment"),
        )
    )

    # Write enriched stream to calls.enriched
    out_enriched = enriched.select(to_json(struct(*enriched.columns)).alias("value"))
    qs_enriched = (
        out_enriched.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", BROKERS)
        .option("topic", ENRICHED_TOPIC)
        .option("checkpointLocation", "/tmp/spark-checkpoints/agent-assist-enriched")
        .outputMode("update")
        .start()
    )

    # Window aggregation for sentiment analysis
    # Combine all texts in window for RAG querying
    windowed = (
        enriched
        .withWatermark("event_timestamp", f"{WINDOW_SECONDS * 2} seconds")
        .groupBy(
            window(col("event_timestamp"), f"{WINDOW_SECONDS} seconds", f"{WINDOW_SECONDS} seconds"),
            col("call_id")
        )
        .agg(
            avg(col("sentiment.score")).alias("avg_sentiment_score"),
            count("*").alias("utterance_count"),
            # Combine all texts with separator for RAG querying
            concat_ws(" | ", collect_list(col("text"))).alias("combined_text"),
            collect_list(struct(
                col("utterance_id"),
                col("text"),
                col("sentiment")
            )).alias("utterances"),
            spark_max(col("event_time")).alias("window_end_time"),
            spark_min(col("event_time")).alias("window_start_time")
        )
        .select(
            col("call_id"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("window_start_time"),
            col("window_end_time"),
            col("avg_sentiment_score"),
            col("utterance_count"),
            col("combined_text"),
            col("utterances")
        )
    )

    # Write windowed sentiment to calls.sentiment
    out_sentiment = windowed.select(
        col("call_id").alias("key"),
        to_json(struct(*windowed.columns)).alias("value")
    )
    
    qs_sentiment = (
        out_sentiment.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", BROKERS)
        .option("topic", SENTIMENT_TOPIC)
        .option("checkpointLocation", "/tmp/spark-checkpoints/agent-assist-sentiment")
        .outputMode("update")
        .start()
    )

    # Wait for both streams
    qs_enriched.awaitTermination()
    qs_sentiment.awaitTermination()


if __name__ == "__main__":
    main()


