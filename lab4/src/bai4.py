from pyspark.sql import functions as F

from utils import create_spark, print_table, read_csv


spark = create_spark("Orders By Year Month Analysis")

orders = read_csv(spark, "Orders.csv")
order_time = F.to_timestamp(F.col("Order_Purchase_Timestamp"), "yyyy-MM-dd HH:mm")

result = (
    orders.select(
        F.year(order_time).alias("Order_Year"),
        F.month(order_time).alias("Order_Month"),
    )
    .where(F.col("Order_Year").isNotNull() & F.col("Order_Month").isNotNull())
    .groupBy("Order_Year", "Order_Month")
    .agg(F.count("*").alias("Order_Count"))
    .orderBy(F.asc("Order_Year"), F.desc("Order_Month"))
)

rows = []

for row in result.collect():
    rows.append([
        row["Order_Year"],
        row["Order_Month"],
        row["Order_Count"],
    ])

print_table(rows, ["Year", "Month", "Order Count"], "Orders by year and month")

spark.stop()