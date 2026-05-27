from pyspark.sql import functions as F

from lab4.src.utils import create_spark, print_table, read_csv


spark = create_spark("Review Score Analysis")

reviews = read_csv(spark, "Order_Reviews.csv")

valid_reviews = (
    reviews.select(F.col("Review_Score").cast("int").alias("Review_Score"))
    .where(F.col("Review_Score").between(1, 5))
)

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
        row["Average_Score"],
        row["Review_Count"],
    ])

print_table(rows, ["Review Score", "Average Score", "Review Count"], "Review score summary")

spark.stop()