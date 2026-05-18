import os
from pyspark import SparkContext
from tabulate import tabulate


MIN_REVIEWS = 5


def get_data_path(file_name):
    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(root_dir, "data", file_name)


def parse_movie(line):
    try:
        parts = line.strip().split(",", 2)

        if len(parts) < 3:
            return None

        movie_id = parts[0]
        title = parts[1]

        return movie_id, title
    except:
        return None


def parse_rating(line):
    try:
        parts = line.strip().split(",")

        if len(parts) < 4:
            return None

        movie_id = parts[1]
        rating = float(parts[2])

        return movie_id, (rating, 1)
    except:
        return None


sc = SparkContext("local[*]", "Movie Rating RDD")
sc.setLogLevel("ERROR")

movies = sc.textFile(get_data_path("movies.txt")) \
    .map(parse_movie) \
    .filter(lambda x: x is not None)

movie_titles = movies.collectAsMap()

ratings_1 = sc.textFile(get_data_path("ratings_1.txt"))
ratings_2 = sc.textFile(get_data_path("ratings_2.txt"))

ratings = ratings_1.union(ratings_2) \
    .map(parse_rating) \
    .filter(lambda x: x is not None)

rating_stats = ratings.reduceByKey(
    lambda a, b: (a[0] + b[0], a[1] + b[1])
)

movie_scores = rating_stats.mapValues(
    lambda x: (x[0] / x[1], x[1])
)

valid_movies = movie_scores.filter(
    lambda x: x[1][1] >= MIN_REVIEWS
)

if valid_movies.isEmpty():
    print("No movie has enough reviews.")
else:
    best_movie = valid_movies.max(
        key=lambda x: x[1][0]
    )

    best_movie_id = best_movie[0]
    best_avg = best_movie[1][0]
    best_count = best_movie[1][1]
    best_title = movie_titles.get(best_movie_id, "Unknown")

    print("Best movie:")
    print(f"ID: {best_movie_id}")
    print(f"Title: {best_title}")
    print(f"Average rating: {best_avg:.2f}")
    print(f"Total reviews: {best_count}")

    top_movies = valid_movies.takeOrdered(
        10,
        key=lambda x: -x[1][0]
    )

    table = []

    for movie_id, value in top_movies:
        avg_rating, review_count = value
        title = movie_titles.get(movie_id, "Unknown")
        table.append([
            movie_id,
            title,
            f"{avg_rating:.2f}",
            review_count
        ])

    print("\nTop movies:")
    print(tabulate(
        table,
        headers=["ID", "Title", "Average Rating", "Total Reviews"],
        tablefmt="grid"
    ))

sc.stop()
