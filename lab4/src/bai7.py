from pyspark.sql import functions as F

from utils import create_spark, print_table, read_csv


spark = create_spark("Top Selling Product Review Analysis")

order_items = read_csv(spark, "Order_Items.csv")
products = read_csv(spark, "Products.csv")
reviews = read_csv(spark, "Order_Reviews.csv")

valid_reviews = (
    reviews.select(
        "Order_ID",
        F.col("Review_Score").cast("int").alias("Review_Score"),
    )
    .where(F.col("Review_Score").between(1, 5))
)

sales_by_product = (
    order_items.groupBy("Product_ID")
    .agg(F.count("*").alias("Quantity_Sold"))
)

ratings_by_product = (
    order_items.join(valid_reviews, "Order_ID", "left")
    .groupBy("Product_ID")
    .agg(F.round(F.avg("Review_Score"), 2).alias("Average_Review_Score"))
)

result = (
    sales_by_product.join(ratings_by_product, "Product_ID", "left")
    .join(products.select("Product_ID", "Product_Category_Name"), "Product_ID", "left")
    .orderBy(F.desc("Quantity_Sold"), F.asc("Product_ID"))
)

top_product = result.first()

if top_product is not None:
    print("Highest sold product:")
    print(f"Product ID: {top_product['Product_ID']}")
    print(f"Category: {top_product['Product_Category_Name']}")
    print(f"Quantity sold: {top_product['Quantity_Sold']}")
    print(f"Average review score: {top_product['Average_Review_Score']}")
    print()

rows = []

for row in result.limit(20).collect():
    rows.append([
        row["Product_ID"],
        row["Product_Category_Name"],
        row["Quantity_Sold"],
        row["Average_Review_Score"],
    ])

print_table(rows, ["Product ID", "Category", "Quantity Sold", "Average Review Score"], "Top products by sales")

spark.stop()