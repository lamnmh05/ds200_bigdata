from utils import create_spark, print_table, read_csv


spark = create_spark("Order_Customer_Seller Overview")

orders = read_csv(spark, "Orders.csv")
customers = read_csv(spark, "Customer_List.csv")
order_items = read_csv(spark, "Order_Items.csv")

rows = [
    ["Orders", orders.select("Order_ID").na.drop().distinct().count()],
    ["Customers", customers.select("Subscriber_ID").na.drop().distinct().count()],
    ["Sellers", order_items.select("Seller_ID").na.drop().distinct().count()],
]

print_table(rows, ["Entity", "Count"], "Summary")

spark.stop()