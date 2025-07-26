from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf, substring
from pyspark.sql.types import *
from keras.models import load_model
from keras.preprocessing.sequence import pad_sequences
import numpy as np
import re, nltk
from nltk.corpus import stopwords
import joblib
from cassandra.cluster import Cluster

try:
    nltk.download('stopwords')
    stop_words = set(stopwords.words('english'))
    # Load LSTM model and tokenizer
    model = load_model("model/model.h5")
    tokenizer = joblib.load("model/tokenizer.pkl")
    print("✅ Model and tokenizer loaded successfully.") 
except Exception as e:
    print(f"❌ Error loading model or tokenizer: {e}")
    raise

# Text cleaning
def preprocess(text):
    if text is None:
        return ""
    text = text.lower()
    text = re.sub(r"http\S+|www\S+|https\S+", '', text)
    text = re.sub(r'\@w+|\#','', text)
    text = re.sub(r'[^A-Za-z\s]', '', text)
    text = ' '.join([word for word in text.split() if word not in stop_words])
    return text

def predict(text):
    max_len = 100
    clean_text = preprocess(text)
    seq = tokenizer.texts_to_sequences([clean_text])
    padded = pad_sequences(seq, maxlen=max_len)
    pred = model.predict(padded)[0][0]
    if pred > 0.6:
        return "positive"
    elif pred < 0.4:
        return "negative"
    else:
        return "neutral"

# Wrap predict with udf
predict_udf = udf(predict, StringType())


# --- SETUP CASSANDRA ---
def setup_cassandra():
    cluster = Cluster(['127.0.0.1'], port=9042)
    session = cluster.connect()
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS kindle_reviews
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
    """)
    session.set_keyspace('kindle_reviews')
    session.execute("""
        CREATE TABLE IF NOT EXISTS reviews (
            reviewer_id text PRIMARY KEY,
            reviewer_name text,
            text text,
            sentiment text,
            review_time text
        )
    """)
    print("✅ Cassandra keyspace and table ready.")
    cluster.shutdown()

setup_cassandra()

# --- SETUP SPARK ---
spark = (
    SparkSession.builder
    .appName("TestKafkaToCassandra")
    .master("local[*]") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,"
            "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1")
    .config("spark.cassandra.connection.host", "localhost")
    .config("spark.cassandra.connection.port", "9042")
    .config("spark.cassandra.auth.username", "cassandra")
    .config("spark.cassandra.auth.password", "cassandra")
    .getOrCreate()
)

# --- DEFINE SCHEMA ---
schema = StructType([
    StructField("reviewerID", StringType()),
    StructField("reviewerName", StringType()),
    StructField("reviewText", StringType()),
    StructField("reviewTime", StringType())
])

# --- READ FROM KAFKA ---
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "kindle-reviews") \
    .option("startingOffsets", "latest") \
    .load()

# --- PARSE AND TRANSFORM ---
parsed_df = df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data"))

# Apply UDF and truncate reviewText
df_with_sentiment = parsed_df \
    .withColumn("sentiment", predict_udf(col("data.reviewText"))) \
    .withColumn("data", parsed_df["data"].withField("reviewText", substring(col("data.reviewText"), 1, 100))) \
    .select(
        col("data.reviewerID").alias("reviewer_id"),
        col("data.reviewerName").alias("reviewer_name"),
        col("data.reviewText").alias("text"),
        col("sentiment"),
        col("data.reviewTime").alias("review_time")
    )

# --- WRITE TO CASSANDRA ---
def write_to_cassandra(batch_df, epoch_id):
    count = batch_df.count()
    print(f"💾 Writing batch {epoch_id} with {count} rows.")
    if count > 0:
        batch_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .options(table="reviews", keyspace="kindle_reviews") \
            .save()

query = df_with_sentiment.writeStream \
    .foreachBatch(write_to_cassandra) \
    .outputMode("update") \
    .start()

print("🚀 Streaming started...")

try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("🛑 Interrupted by user.")
    query.stop()
    spark.stop()
