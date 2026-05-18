import os
from pyspark import SparkContext
from tabulate import tabulate


def get_data_path(file_name):
    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(root_dir, "data", file_name)


def parse_occupation(line):
    try:
        parts = line.strip().split(",", 1)
        occupation_id = parts[0]
        occupation_name = parts[1]
        return occupation_id, occupation_name
    except:
        return None


def parse_user(line):
    try:
        parts = line.strip().split(",")
        user_id = parts[0]
        occupation_id = parts[3]
        return user_id, occupation_id
    except:
        return None


def parse_rating(line):
    try:
        parts = line.strip().split(",")
        user_id = parts[0]
        rating = float(parts[2])
        return user_id, rating
    except:
        return None


sc = SparkContext("local[*]", "Rating By Occupation")
sc.setLogLevel("ERROR")

occupations = sc.textFile(get_data_path("occupation.txt")) \
    .map(parse_occupation) \
    .filter(lambda x: x is not None)

occupation_map = occupations.collectAsMap()

users = sc.textFile(get_data_path("users.txt")) \
    .map(parse_user) \
    .filter(lambda x: x is not None) \
    .map(lambda x: (x[0], occupation_map.get(x[1], "Unknown")))

ratings_1 = sc.textFile(get_data_path("ratings_1.txt"))
ratings_2 = sc.textFile(get_data_path("ratings_2.txt"))

ratings = ratings_1.union(ratings_2) \
    .map(parse_rating) \
    .filter(lambda x: x is not None)

rating_with_occupation = ratings.join(users)

occupation_ratings = rating_with_occupation.map(
    lambda x: (x[1][1], (x[1][0], 1))
)

stats = occupation_ratings.reduceByKey(
    lambda a, b: (a[0] + b[0], a[1] + b[1])
)

result = stats.mapValues(
    lambda x: (x[0] / x[1], x[1])
)

rows = result.takeOrdered(
    100,
    key=lambda x: -x[1][0]
)

table = []

for occupation, value in rows:
    avg_rating, total_reviews = value
    table.append([
        occupation,
        f"{avg_rating:.2f}",
        total_reviews
    ])

print("Average rating by occupation:")
print(tabulate(
    table,
    headers=["Occupation", "Average Rating", "Total Reviews"],
    tablefmt="grid"
))

sc.stop()

