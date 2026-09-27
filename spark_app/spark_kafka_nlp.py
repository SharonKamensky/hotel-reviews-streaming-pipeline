from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, lower, split, explode
from pyspark.sql.types import StructType, StringType
from pyspark.sql import functions as F

# Create SparkSession
spark = SparkSession.builder \
    .appName("HotelReviewsKafkaConsumer") \
    .getOrCreate()

# Read the stream from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "hotel-reviews") \
    .option("startingOffsets", "earliest") \
    .load()

# Deserialize the Kafka value (JSON string)
reviews = df.selectExpr("CAST(value AS STRING) as json_str")

# JSON schema matching the dataset columns
review_schema = StructType() \
    .add("Hotel_Address", StringType()) \
    .add("Additional_Number_of_Scoring", StringType()) \
    .add("Review_Date", StringType()) \
    .add("Average_Score", StringType()) \
    .add("Hotel_Name", StringType()) \
    .add("Reviewer_Nationality", StringType()) \
    .add("Negative_Review", StringType()) \
    .add("Review_Total_Negative_Word_Counts", StringType()) \
    .add("Total_Number_of_Reviews", StringType()) \
    .add("Positive_Review", StringType()) \
    .add("Review_Total_Positive_Word_Counts", StringType()) \
    .add("Total_Number_of_Reviews_Reviewer_Has_Given", StringType()) \
    .add("Reviewer_Score", StringType()) \
    .add("Tags", StringType()) \
    .add("days_since_review", StringType()) \
    .add("lat", StringType()) \
    .add("lng", StringType())

parsed = reviews.withColumn("data", from_json(col("json_str"), review_schema)).select("data.*")

STOPWORDS = [
    "the", "and", "was", "with", "for", "you", "are", "but", "not", "all", "had", "our", "very",
    "this", "that", "they", "her", "him", "she", "his", "your", "its", "from", "out", "who",
    "why", "too", "just", "any", "did", "has", "have", "had", "were", "because", "about",
    "would", "could", "should", "when", "where", "how", "than", "then", "will", "can", "if",
    "also", "so", "we", "he", "on", "in", "is", "at", "to", "of", "it", "my", "me", "as", "by",
    "an", "be", "or", "no", "a", "i"
]

# --- Process positive text (Positive_Review) ---
pos_words = parsed.select(
    "Hotel_Name",
    explode(split(lower(col("Positive_Review")), "\\W+")).alias("word")
).filter(
    (col("word") != "") &
    (~col("word").isin(STOPWORDS))
)
pos_word_counts = pos_words.groupBy("Hotel_Name", "word").count().orderBy(F.desc("count"))

# --- Process negative text (Negative_Review) ---
neg_words = parsed.select(
    "Hotel_Name",
    explode(split(lower(col("Negative_Review")), "\\W+")).alias("word")
).filter(
    (col("word") != "") &
    (~col("word").isin(STOPWORDS))
)
neg_word_counts = neg_words.groupBy("Hotel_Name", "word").count().orderBy(F.desc("count"))

# --- Print results ---
pos_query = pos_word_counts.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .option("numRows", 50) \
    .start()

neg_query = neg_word_counts.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .option("numRows", 20) \
    .start()

pos_query.awaitTermination()
neg_query.awaitTermination()
