from pyspark.sql import functions as F

from lab4.src.utils import create_spark, print_table, read_csv


spark = create_spark("Revenue 2024 By Category")

orders = read_csv(spark, "Orders.csv")
order_items = read_csv(spark, "Order_Items.csv")
products = read_csv(spark, "Products.csv")

order_time = F.to_timestamp(F.col("Order_Purchase_Timestamp"), "yyyy-MM-dd HH:mm")

orders_2024 = (
    orders.select(
        "Order_ID",
        F.year(order_time).alias("Order_Year"),
    )
    .where(F.col("Order_Year") == 2024)
)

result = (
    orders_2024.join(order_items, "Order_ID", "inner")
    .join(products, "Product_ID", "left")
    .where(F.col("Product_Category_Name").isNotNull())
    .withColumn(
        "Revenue",
        F.coalesce(F.col("Price"), F.lit(0)) + F.coalesce(F.col("Freight_Value"), F.lit(0)),
    )
    .groupBy("Product_Category_Name")
    .agg(
        F.round(F.sum("Revenue"), 2).alias("Total_Revenue"),
        F.count("*").alias("Item_Count"),
    )
    .orderBy(F.desc("Total_Revenue"), F.asc("Product_Category_Name"))
)

rows = []

for row in result.collect():
    rows.append([
        row["Product_Category_Name"],
        row["Total_Revenue"],
        row["Item_Count"],
    ])

print_table(rows, ["Category", "Total Revenue", "Item Count"], "2024 revenue by category")

spark.stop()