from pyspark.sql import functions as F

from utils import create_spark, print_table, read_csv


spark = create_spark("Lab4 Bai5")

reviews = read_csv(spark, "Order_Reviews.csv")

cleaned_reviews = reviews.select(
    F.col("Review_Score").cast("double").alias("Review_Score")
)

valid_reviews = cleaned_reviews.where(
    F.col("Review_Score").isNotNull()
    & F.col("Review_Score").between(1, 5)
    & (F.col("Review_Score") == F.floor(F.col("Review_Score")))
).select(F.col("Review_Score").cast("int").alias("Review_Score"))

summary = (
    valid_reviews.groupBy("Review_Score")
    .agg(
        F.round(F.avg("Review_Score"), 2).alias("Average_Score"),
        F.count("*").alias("Review_Count"),
    )
)

levels = spark.createDataFrame(
    [(1,), (2,), (3,), (4,), (5,)],
    ["Review_Score"],
)

result = (
    levels.join(summary, "Review_Score", "left")
    .orderBy("Review_Score")
    .na.fill({"Review_Count": 0})
)

rows = []

for row in result.collect():
    rows.append([
        row["Review_Score"],
        row["Review_Count"],
    ])

print_table(rows, ["Review Score", "Review Count"], "Review score summary")

spark.stop()