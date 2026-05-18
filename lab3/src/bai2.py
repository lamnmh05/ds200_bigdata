import os
from pyspark import SparkContext
from tabulate import tabulate


def get_data_path(file_name):
    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(root_dir, "data", file_name)


def parse_movie(line):
    try:
        parts = line.strip().split(",", 2)
        movie_id = parts[0]
        genres = parts[2].split("|")

        return movie_id, genres
    except:
        return None


def parse_rating(line):
    try:
        parts = line.strip().split(",")
        movie_id = parts[1]
        rating = float(parts[2])

        return movie_id, rating
    except:
        return None


def to_genre_rating(row):
    rating = row[1][0]
    genres = row[1][1]

    result = []

    for genre in genres:
        result.append((genre, (rating, 1)))

    return result


sc = SparkContext("local[*]", "Genre Rating")
sc.setLogLevel("ERROR")

movies = sc.textFile(get_data_path("movies.txt")) \
    .map(parse_movie) \
    .filter(lambda x: x is not None)

ratings_1 = sc.textFile(get_data_path("ratings_1.txt"))
ratings_2 = sc.textFile(get_data_path("ratings_2.txt"))

ratings = ratings_1.union(ratings_2) \
    .map(parse_rating) \
    .filter(lambda x: x is not None)

genre_ratings = ratings.join(movies) \
    .flatMap(to_genre_rating)

genre_stats = genre_ratings.reduceByKey(
    lambda a, b: (a[0] + b[0], a[1] + b[1])
)

genre_scores = genre_stats.mapValues(
    lambda x: (x[0] / x[1], x[1])
)

top_genres = genre_scores.takeOrdered(
    100,
    key=lambda x: -x[1][0]
)

table = []

for genre, value in top_genres:
    avg_rating, total_reviews = value
    table.append([
        genre,
        f"{avg_rating:.2f}",
        total_reviews
    ])

print("Average rating by genre:")
print(tabulate(
    table,
    headers=["Genre", "Average Rating", "Total Reviews"],
    tablefmt="grid"
))

sc.stop()
