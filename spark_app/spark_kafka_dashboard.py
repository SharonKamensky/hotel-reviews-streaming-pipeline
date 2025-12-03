from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, lower, split, explode, to_date, month, avg, count, desc
from pyspark.sql.types import StructType, StringType, DoubleType
from pyspark.sql import functions as F

# ---- הגדרת SparkSession ----
spark = SparkSession.builder \
    .appName("HotelReviewsDashboard") \
    .getOrCreate()

# ---- קריאה מ־Kafka ----
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "hotel-reviews") \
    .option("startingOffsets", "earliest") \
    .load()

# ---- סכימת JSON ----
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

parsed = df.selectExpr("CAST(value AS STRING) as json_str") \
    .withColumn("data", from_json(col("json_str"), review_schema)) \
    .select("data.*")

# ---- רשימת Stopwords בסיסית ----
STOPWORDS = [
    "the", "and", "was", "with", "for", "you", "are", "but", "not", "all", "had", "our", "very",
    "this", "that", "they", "her", "him", "she", "his", "your", "its", "from", "out", "who",
    "why", "too", "just", "any", "did", "has", "have", "had", "were", "because", "about",
    "would", "could", "should", "when", "where", "how", "than", "then", "will", "can", "if",
    "also", "so", "we", "he", "on", "in", "is", "at", "to", "of", "it", "my", "me", "as", "by",
    "an", "be", "or", "no", "a", "i"
]

# ---- ניתוח 1: שביעות רצון ממוצעת לכל מלון ----
avg_score = parsed.groupBy("Hotel_Name").agg(
    F.avg(col("Reviewer_Score").cast(DoubleType())).alias("avg_score"),
    count("*").alias("num_reviews")
).orderBy(desc("avg_score"))

# ---- ניתוח 2: מילים נפוצות חיוביות לכל מלון ----
pos_words = parsed.select(
    "Hotel_Name",
    explode(split(lower(col("Positive_Review")), "\\W+")).alias("word")
).filter(
    (col("word") != "") &
    (~col("word").isin(STOPWORDS))
)
pos_word_counts = pos_words.groupBy("Hotel_Name", "word").count()
# כאן אפשר להמשיך (בהמשך) – לבחור את 3–5 המילים המובילות לכל מלון לפי count

# ---- ניתוח 3: מילים נפוצות שליליות לכל מלון ----
neg_words = parsed.select(
    "Hotel_Name",
    explode(split(lower(col("Negative_Review")), "\\W+")).alias("word")
).filter(
    (col("word") != "") &
    (~col("word").isin(STOPWORDS))
)
neg_word_counts = neg_words.groupBy("Hotel_Name", "word").count()
# כנ"ל – אפשר להוציא TOP 3–5 למלון

# ---- ניתוח 4: גרף שביעות רצון לאורך זמן (עונתי) ----
parsed = parsed.withColumn("Review_Date", to_date("Review_Date", "M/d/yyyy"))
parsed = parsed.withColumn("month", month("Review_Date"))

seasonal_score = parsed.groupBy("Hotel_Name", "month").agg(
    avg(col("Reviewer_Score").cast(DoubleType())).alias("monthly_avg_score"),
    count("*").alias("monthly_num_reviews")
).orderBy("Hotel_Name", "month")

# ---- דוגמה להדפסה/שמירה של כל הניתוחים (לשלב הבדיקות) ----
# (כמובן אפשר להוציא ל־CSV/Parquet לכל דשבורד/כלי BI)

score_query = avg_score.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "outputs/avg_score/") \
    .option("checkpointLocation", "outputs/avg_score_checkpoint/") \
    .start()

pos_query = pos_word_counts.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "outputs/pos_words/") \
    .option("checkpointLocation", "outputs/pos_words_checkpoint/") \
    .start()

neg_query = neg_word_counts.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "outputs/neg_words/") \
    .option("checkpointLocation", "outputs/neg_words_checkpoint/") \
    .start()

seasonal_query = seasonal_score.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "outputs/seasonal/") \
    .option("checkpointLocation", "outputs/seasonal_checkpoint/") \
    .start()

score_query.awaitTermination()
pos_query.awaitTermination()
neg_query.awaitTermination()
seasonal_query.awaitTermination()
