from lab4.src.utils import create_spark, read_csv


TABLES = [
    ("Orders", "Orders.csv"),
    ("Customer_List", "Customer_List.csv"),
    ("Order_Items", "Order_Items.csv"),
    ("Products", "Products.csv"),
    ("Order_Reviews", "Order_Reviews.csv"),
]


spark = create_spark("Read data")

for title, file_name in TABLES:
    dataframe = read_csv(spark, file_name)
    print(f"{title} schema:")
    dataframe.printSchema()
    print(f"{title} rows: {dataframe.count()}")
    print()

spark.stop()