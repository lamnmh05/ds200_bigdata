import os
from datetime import datetime
from pyspark import SparkContext
from tabulate import tabulate


def get_data_path(file_name):
    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(root_dir, "data", file_name)


def timestamp_to_year(timestamp):
    return datetime.utcfromtimestamp(int(timestamp)).year


def parse_rating(line):
    try:
        parts = line.strip().split(",")
        rating = float(parts[2])
        timestamp = parts[3]
        year = timestamp_to_year(timestamp)
        return year, (rating, 1)
    except:
        return None


sc = SparkContext("local[*]", "Rating By Year")
sc.setLogLevel("ERROR")

ratings_1 = sc.textFile(get_data_path("ratings_1.txt"))
ratings_2 = sc.textFile(get_data_path("ratings_2.txt"))

ratings = ratings_1.union(ratings_2) \
    .map(parse_rating) \
    .filter(lambda x: x is not None)

stats = ratings.reduceByKey(
    lambda a, b: (a[0] + b[0], a[1] + b[1])
)

result = stats.mapValues(
    lambda x: (x[0] / x[1], x[1])
)

rows = result.takeOrdered(
    100,
    key=lambda x: x[0]
)

table = []

for year, value in rows:
    avg_rating, total_reviews = value
    table.append([
        year,
        f"{avg_rating:.2f}",
        total_reviews
    ])

print("Average rating by year:")
print(tabulate(
    table,
    headers=["Year", "Average Rating", "Total Reviews"],
    tablefmt="grid"
))

sc.stop()

