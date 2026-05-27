import os

from pyspark.sql import SparkSession
from tabulate import tabulate


def get_data_path(file_name):
    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(root_dir, "data", file_name)


def create_spark(app_name):
    return SparkSession.builder.master("local[*]").appName(app_name).getOrCreate()


def read_csv(spark, file_name):
    return spark.read.options(
        header=True,
        inferSchema=True,
        sep=";",
        quote='"',
        escape='"'
    ).csv(get_data_path(file_name))


def print_table(rows, headers, title):
    print(title)
    print(tabulate(rows, headers=headers, tablefmt="grid"))