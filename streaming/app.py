import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json, to_json, struct, udf, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, MapType


RAW_TOPIC = os.getenv("KAFKA_TOPIC_RAW", "calls.raw")
ENRICHED_TOPIC = os.getenv("KAFKA_TOPIC_ENRICHED", "calls.enriched")
BROKERS = os.getenv("KAFKA_BROKERS", "kafka:29092")

raw_schema = StructType([
    StructField("call_id", StringType(), True),
    StructField("utterance_id", StringType(), True),
    StructField("utterance_index", IntegerType(), True),
    StructField("timestamp_ms", LongType(), True),
    StructField("speaker_role", StringType(), True),
    StructField("text", StringType(), True),
    StructField("metadata", MapType(StringType(), StringType()), True),
])

def clean_text(txt: str) -> str:
    if not txt:
        return ""
    t = txt.strip().replace("\n", " ")
    return " ".join(t.split())

def infer_role(txt: str, role: str | None) -> str:
    if role:
        return role
    t = (txt or "").lower()
    if t.startswith("agent:"):
        return "agent"
    if t.startswith("customer:"):
        return "customer"
    return "unknown"

def simple_sentiment(txt: str) -> tuple[float, str]:
    # Placeholder lightweight heuristic; replace with HF pipeline in production
    t = (txt or "").lower()
    score = 0.0
    if any(w in t for w in ["great", "thanks", "appreciate", "good", "awesome", "perfect"]):
        score = 0.7
    elif any(w in t for w in ["angry", "upset", "terrible", "bad", "cancel", "complaint"]):
        score = -0.7
    label = "POS" if score > 0.2 else ("NEG" if score < -0.2 else "NEU")
    return float(score), label

from pyspark.sql.types import StructType as S, StructField as F, DoubleType
sentiment_schema = S([F("score", DoubleType(), False), F("label", StringType(), False)])

from pyspark.sql.functions import pandas_udf, PandasUDFType
import pandas as pd

@pandas_udf(sentiment_schema, PandasUDFType.SCALAR)
def sentiment_udf(texts: pd.Series) -> pd.DataFrame:
    scores = []
    labels = []
    for t in texts:
        sc, lb = simple_sentiment(t)
        scores.append(sc)
        labels.append(lb)
    return pd.DataFrame({"score": scores, "label": labels})

clean_udf = udf(clean_text, StringType())
role_udf = udf(infer_role, StringType())


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

    enriched = (
        parsed
        .withColumn("clean_text", clean_udf(col("text")))
        .withColumn("speaker_role", role_udf(col("text"), col("speaker_role")))
        .withColumn("sentiment", sentiment_udf(col("clean_text")))
        .select(
            col("call_id"),
            col("utterance_id"),
            col("utterance_index"),
            col("timestamp_ms"),
            col("speaker_role"),
            col("clean_text").alias("text"),
            col("metadata"),
            col("sentiment"),
        )
    )

    out = enriched.select(to_json(struct(*enriched.columns)).alias("value"))

    qs = (
        out.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", BROKERS)
        .option("topic", ENRICHED_TOPIC)
        .option("checkpointLocation", "/tmp/spark-checkpoints/agent-assist")
        .outputMode("update")
        .start()
    )
    qs.awaitTermination()


if __name__ == "__main__":
    main()


