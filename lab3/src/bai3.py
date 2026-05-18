import os
from pyspark import SparkContext
from tabulate import tabulate


def get_data_path(file_name):
    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(root_dir, "data", file_name)


def parse_movie(line):
    try:
        parts = line.strip().split(",", 2)
        return parts[0], parts[1]
    except:
        return None


def parse_user(line):
    try:
        parts = line.strip().split(",")
        user_id = parts[0]
        gender = parts[1]
        return user_id, gender
    except:
        return None


def parse_rating(line):
    try:
        parts = line.strip().split(",")
        user_id = parts[0]
        movie_id = parts[1]
        rating = float(parts[2])
        return user_id, (movie_id, rating)
    except:
        return None


sc = SparkContext("local[*]", "Movie Rating By Gender")
sc.setLogLevel("ERROR")

movies = sc.textFile(get_data_path("movies.txt")) \
    .map(parse_movie) \
    .filter(lambda x: x is not None)

movie_titles = movies.collectAsMap()

users = sc.textFile(get_data_path("users.txt")) \
    .map(parse_user) \
    .filter(lambda x: x is not None)

ratings_1 = sc.textFile(get_data_path("ratings_1.txt"))
ratings_2 = sc.textFile(get_data_path("ratings_2.txt"))

ratings = ratings_1.union(ratings_2) \
    .map(parse_rating) \
    .filter(lambda x: x is not None)

rating_with_gender = ratings.join(users)

movie_gender_rating = rating_with_gender.map(
    lambda x: ((x[1][0][0], x[1][1]), (x[1][0][1], 1))
)

stats = movie_gender_rating.reduceByKey(
    lambda a, b: (a[0] + b[0], a[1] + b[1])
)

result = stats.mapValues(
    lambda x: (x[0] / x[1], x[1])
)

rows = result.takeOrdered(
    100,
    key=lambda x: (x[0][0], x[0][1])
)

table = []

for key, value in rows:
    movie_id, gender = key
    avg_rating, total_reviews = value
    title = movie_titles.get(movie_id, "Unknown")
    table.append([
        movie_id,
        title,
        gender,
        f"{avg_rating:.2f}",
        total_reviews
    ])

print("Average rating of each movie by gender:")
print(tabulate(
    table,
    headers=["Movie ID", "Title", "Gender", "Average Rating", "Total Reviews"],
    tablefmt="grid"
))

sc.stop()

