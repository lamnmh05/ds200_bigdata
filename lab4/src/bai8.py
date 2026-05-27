from pyspark.sql import functions as F

from lab4.src.utils import create_spark, print_table, read_csv


spark = create_spark("Delivery Delay Analysis")

orders = read_csv(spark, "Orders.csv")
order_items = read_csv(spark, "Order_Items.csv")

delivered_time = F.to_timestamp(F.col("Order_Delivered_Carrier_Date"), "yyyy-MM-dd HH:mm")
limit_time = F.to_timestamp(F.col("Shipping_Limit_Date"), "yyyy-MM-dd HH:mm")

delivery = (
    orders.select(
        "Order_ID",
        delivered_time.alias("Delivered_Carrier_Time"),
    )
    .join(
        order_items.select(
            "Order_ID",
            limit_time.alias("Shipping_Limit_Time"),
        ),
        "Order_ID",
        "inner",
    )
    .where(
        F.col("Delivered_Carrier_Time").isNotNull()
        & F.col("Shipping_Limit_Time").isNotNull()
    )
    .withColumn(
        "Delay_Days",
        F.datediff(
            F.to_date(F.col("Delivered_Carrier_Time")),
            F.to_date(F.col("Shipping_Limit_Time")),
        ),
    )
)

summary = delivery.agg(
    F.round(F.avg("Delay_Days"), 2).alias("Average_Delay_Days"),
    F.min("Delay_Days").alias("Min_Delay_Days"),
    F.max("Delay_Days").alias("Max_Delay_Days"),
    F.count("*").alias("Item_Count"),
).collect()[0]

summary_rows = [
    ["Average delay days", summary["Average_Delay_Days"]],
    ["Min delay days", summary["Min_Delay_Days"]],
    ["Max delay days", summary["Max_Delay_Days"]],
    ["Delivery items", summary["Item_Count"]],
]

print_table(summary_rows, ["Metric", "Value"], "Delivery summary")

status_result = (
    delivery.withColumn(
        "Delivery_Status",
        F.when(F.col("Delay_Days") < 0, "Early")
        .when(F.col("Delay_Days") == 0, "On time")
        .otherwise("Late"),
    )
    .groupBy("Delivery_Status")
    .agg(F.count("*").alias("Item_Count"))
    .orderBy(F.desc("Item_Count"), F.asc("Delivery_Status"))
)

status_rows = []

for row in status_result.collect():
    status_rows.append([
        row["Delivery_Status"],
        row["Item_Count"],
    ])

print_table(status_rows, ["Status", "Item Count"], "Delivery status")
print()

worst_rows = []

for row in delivery.orderBy(F.desc("Delay_Days"), F.asc("Order_ID")).limit(20).collect():
    worst_rows.append([
        row["Order_ID"],
        row["Shipping_Limit_Time"],
        row["Delivered_Carrier_Time"],
        row["Delay_Days"],
    ])

print_table(worst_rows, ["Order ID", "Shipping Limit", "Delivered Carrier", "Delay Days"], "Worst delivery delays")

spark.stop()