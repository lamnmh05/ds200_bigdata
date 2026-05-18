import os
from pyspark import SparkContext
from tabulate import tabulate


def get_data_path(file_name):
    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(root_dir, "data", file_name)


def get_age_group(age):
    if age < 18:
        return "Under 18"
    if age <= 24:
        return "18-24"
    if age <= 34:
        return "25-34"
    if age <= 44:
        return "35-44"
    if age <= 54:
        return "45-54"
    return "55+"


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
        age = int(parts[2])
        age_group = get_age_group(age)
        return user_id, age_group
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


sc = SparkContext("local[*]", "Movie Rating By Age Group")
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

rating_with_age_group = ratings.join(users)

movie_age_rating = rating_with_age_group.map(
    lambda x: ((x[1][0][0], x[1][1]), (x[1][0][1], 1))
)

stats = movie_age_rating.reduceByKey(
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
    movie_id, age_group = key
    avg_rating, total_reviews = value
    title = movie_titles.get(movie_id, "Unknown")
    table.append([
        movie_id,
        title,
        age_group,
        f"{avg_rating:.2f}",
        total_reviews
    ])

print("Average rating of each movie by age group:")
print(tabulate(
    table,
    headers=["Movie ID", "Title", "Age Group", "Average Rating", "Total Reviews"],
    tablefmt="grid"
))

sc.stop()

