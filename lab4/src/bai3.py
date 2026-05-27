from pyspark.sql import functions as F

from utils import create_spark, print_table, read_csv


spark = create_spark("Orders By Country Analysis")

orders = read_csv(spark, "Orders.csv")
customers = read_csv(spark, "Customer_List.csv")

result = (
    orders.join(customers, "Customer_Trx_ID", "left")
    .where(F.col("Customer_Country").isNotNull())
    .groupBy("Customer_Country")
    .agg(F.count("*").alias("Order_Count"))
    .orderBy(F.desc("Order_Count"), F.asc("Customer_Country"))
)

rows = []

for row in result.collect():
    rows.append([
        row["Customer_Country"],
        row["Order_Count"],
    ])

print_table(rows, ["Country", "Order Count"], "Orders by country")

spark.stop()