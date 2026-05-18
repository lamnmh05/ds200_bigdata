import os
from pyspark import SparkContext


MIN_REVIEWS = 50


def data_path(file_name):
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base_dir, "data", file_name)


def parse_movie(line):
    try:
        movie_id, rest = line.strip().split(",", 1)
        title = rest.rsplit(",", 1)[0]
        return movie_id, title
    except:
        return None


def parse_rating(line):
    try:
        parts = line.strip().split(",")
        movie_id = parts[1]
        rating = float(parts[2])
        return movie_id, (rating, 1)
    except:
        return None


sc = SparkContext("local[*]", "Movie Rating RDD")
sc.setLogLevel("ERROR")

movies = sc.textFile(data_path("movies.txt")) \
    .map(parse_movie) \
    .filter(lambda x: x is not None)

movie_titles = movies.collectAsMap()

ratings_1 = sc.textFile(data_path("ratings_1.txt"))
ratings_2 = sc.textFile(data_path("ratings_2.txt"))

ratings = ratings_1.union(ratings_2) \
    .map(parse_rating) \
    .filter(lambda x: x is not None)

total_by_movie = ratings.reduceByKey(
    lambda a, b: (a[0] + b[0], a[1] + b[1])
)

avg_by_movie = total_by_movie.mapValues(
    lambda x: (x[0] / x[1], x[1])
)

qualified_movies = avg_by_movie.filter(
    lambda x: x[1][1] >= MIN_REVIEWS
)

if qualified_movies.isEmpty():
    print("No movie has enough reviews.")
else:
    best_movie = qualified_movies.max(
        key=lambda x: x[1][0]
    )

    movie_id = best_movie[0]
    avg_score = best_movie[1][0]
    review_count = best_movie[1][1]
    title = movie_titles.get(movie_id, "Unknown")

    print("Best movie:")
    print(f"ID: {movie_id}")
    print(f"Title: {title}")
    print(f"Average rating: {avg_score:.2f}")
    print(f"Total reviews: {review_count}")

    print("\nTop movies:")

    top_movies = qualified_movies.takeOrdered(
        10,
        key=lambda x: -x[1][0]
    )

    for movie_id, value in top_movies:
        avg_score, review_count = value
        title = movie_titles.get(movie_id, "Unknown")
        print(f"{movie_id} | {title} | {avg_score:.2f} | {review_count}")

sc.stop()