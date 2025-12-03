from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, split, explode, to_date, month, avg, count, desc
from pyspark.sql.types import StructType, StringType, DoubleType
from pyspark.sql import functions as F

# ---- הגדרת SparkSession ----
spark = SparkSession.builder \
    .appName("HotelReviewsBatchAnalysis") \
    .getOrCreate()

# ---- קריאה מקובץ CSV (לא מ־Kafka) ----
df = spark.read.option("header", True).csv("/app/spark_app/Hotel_Reviews.csv")

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
avg_score = df.groupBy("Hotel_Name").agg(
    F.avg(col("Reviewer_Score").cast(DoubleType())).alias("avg_score"),
    count("*").alias("num_reviews")
).orderBy(desc("avg_score"))

# ---- ניתוח 2: מילים נפוצות חיוביות לכל מלון ----
pos_words = df.select(
    "Hotel_Name",
    explode(split(lower(col("Positive_Review")), "\\W+")).alias("word")
).filter(
    (col("word") != "") &
    (~col("word").isin(STOPWORDS))
)
pos_word_counts = pos_words.groupBy("Hotel_Name", "word").count().orderBy("Hotel_Name", desc("count"))

# ---- ניתוח 3: מילים נפוצות שליליות לכל מלון ----
neg_words = df.select(
    "Hotel_Name",
    explode(split(lower(col("Negative_Review")), "\\W+")).alias("word")
).filter(
    (col("word") != "") &
    (~col("word").isin(STOPWORDS))
)
neg_word_counts = neg_words.groupBy("Hotel_Name", "word").count().orderBy("Hotel_Name", desc("count"))

# ---- ניתוח 4: גרף שביעות רצון לאורך זמן (עונתי) ----
df = df.withColumn("Review_Date", to_date("Review_Date", "M/d/yyyy"))
df = df.withColumn("month", month("Review_Date"))

seasonal_score = df.groupBy("Hotel_Name", "month").agg(
    avg(col("Reviewer_Score").cast(DoubleType())).alias("monthly_avg_score"),
    count("*").alias("monthly_num_reviews")
).orderBy("Hotel_Name", "month")

# ---- שמירת התוצאות ל־CSV ----
avg_score.write.mode("overwrite").option("header", True).csv("/app/spark_app/outputs/avg_score/")
pos_word_counts.write.mode("overwrite").option("header", True).csv("/app/spark_app/outputs/pos_words/")
neg_word_counts.write.mode("overwrite").option("header", True).csv("/app/spark_app/outputs/neg_words/")
seasonal_score.write.mode("overwrite").option("header", True).csv("/app/spark_app/outputs/seasonal/")

print("Analysis finished! Results saved in /app/spark_app/outputs/")
