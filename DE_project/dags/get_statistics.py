import pandas as pd
import airflow.utils.dates
import numpy as np
import pyspark.sql.types as t

from minio import Minio
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as f
from os import environ

args = {
    'owner': 'admin',
    'start_date': datetime(2022, 6, 8),
    'provide_context': True,
}

POSTGRES_USER = environ.get("POSTGRES_USER")
POSTGRES_PASSWORD = environ.get("POSTGRES_PASSWORD")
POSTGRES_HOST = environ.get("POSTGRES_HOST")
POSTGRES_DEFAULT_DB = environ.get("POSTGRES_DEFAULT_DB")
POSTGRES_PORT = environ.get("POSTGRES_PORT")

MINIO_ACCESS_KEY = environ.get("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = environ.get("MINIO_SECRET_KEY")
MINIO_HOST = environ.get("MINIO_HOST")


def get_minio():
    """Returns connection with Minio"""
    return Minio(
        endpoint=MINIO_HOST,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )


class FilmDBSchemas:
    """Class contains schemas for the film database"""

    def __init__(self):
        self.name_basics_schema = t.StructType(
            [t.StructField('nconst', t.StringType(), True),
             t.StructField('primaryName', t.StringType(), True),
             t.StructField('birthYear', t.IntegerType(), True),
             t.StructField('deathYear', t.IntegerType(), True),
             t.StructField('primaryProfession', t.StringType(), True),
             t.StructField('knownForTitles', t.StringType(), True)])

        self.title_akas_schema = t.StructType(
            [t.StructField('titleId', t.StringType(), True),
             t.StructField('ordering', t.IntegerType(), True),
             t.StructField('title', t.StringType(), True),
             t.StructField('region', t.StringType(), True),
             t.StructField('language', t.StringType(), True),
             t.StructField('types', t.StringType(), True),
             t.StructField('attributes', t.StringType(), True),
             t.StructField('isOriginalTitle', t.IntegerType(), True)])

        self.title_crew_schema = t.StructType(
            [t.StructField('tconst', t.StringType(), True),
             t.StructField('directors', t.StringType(), True),
             t.StructField('writers', t.StringType(), True)])

        self.title_basics_schema = t.StructType(
            [t.StructField('tconst', t.StringType(), True),
             t.StructField('titleType', t.StringType(), True),
             t.StructField('primaryTitle', t.StringType(), True),
             t.StructField('originalTitle', t.StringType(), True),
             t.StructField('isAdult', t.IntegerType(), True),
             t.StructField('startYear', t.IntegerType(), True),
             t.StructField('endYear', t.IntegerType(), True),
             t.StructField('runtimeMinutes', t.IntegerType(), True),
             t.StructField('genres', t.StringType(), True)])

        self.title_episode_schema = t.StructType(
            [t.StructField('tconst', t.StringType(), True),
             t.StructField('parentTconst', t.StringType(), True),
             t.StructField('seasonNumber', t.IntegerType(), True),
             t.StructField('episodeNumber', t.IntegerType(), True)
             ])

        self.title_principals_schema = t.StructType(
            [t.StructField('tconst', t.StringType(), True),
             t.StructField('ordering', t.StringType(), True),
             t.StructField('nconst', t.StringType(), True),
             t.StructField('category', t.StringType(), True),
             t.StructField('job', t.StringType(), True),
             t.StructField('characters', t.StringType(), True)
             ])

        self.title_ratings_schema = t.StructType(
            [t.StructField('tconst', t.StringType(), True),
             t.StructField('averageRating', t.FloatType(), True),
             t.StructField('numVotes', t.IntegerType(), True)
             ])


class FilmDB:
    """Database of films"""

    def __init__(self, schema):
        self.top_5_of_director = None
        self.top_actors = None
        self.top_10_each_genre_decades = None
        self.top_10_each_genre = None
        self.sixtys_top_films = None
        self.last_decade_top_films = None
        self.all_time_top_films = None
        print('SparkSession.builder.appName')
        self.spark = SparkSession.builder.appName('FilmDB').getOrCreate()

        self.name_basics = self.read_from_csv('name_basics.tsv', schema.name_basics_schema)
        self.title_akas = self.read_from_csv('title_akas.tsv', schema.title_akas_schema)
        self.title_crew = self.read_from_csv('title_crew.tsv', schema.title_crew_schema)
        self.title_basics = self.read_from_csv('title_basics.tsv', schema.title_basics_schema)
        self.title_episode = self.read_from_csv('title_episode.tsv', schema.title_episode_schema)
        self.title_principals = self.read_from_csv('title_principals.tsv', schema.title_principals_schema)
        self.title_ratings = self.read_from_csv('title_ratings.tsv', schema.title_ratings_schema)

    def read_from_csv(self, file_name, csv_schema):
        minio_client = get_minio()
        minio_obj = minio_client.get_object("imdb", file_name)
        pd_df = pd.read_csv(
            minio_obj,
            lineterminator='\n',
            sep='\t',
            header=0, dtype=object, na_filter=False
        )
        print("pd df read")
        empty_rdd = self.spark.sparkContext.emptyRDD()
        list_df = np.array_split(pd_df, 100)
        df = self.spark.createDataFrame(empty_rdd, csv_schema)

        for idx, chunk in enumerate(list_df):
            if idx > 90:
                spark_df = self.spark.createDataFrame(chunk)
                df = df.union(spark_df)
                print('chunk added')
        print(df.printSchema)
        return df

    def get_all_time_top_films(self):
        """Returns all times top 10 films"""
        self.all_time_top_films = (
            self.title_basics.join(self.title_ratings, on='tconst', how='left').
            select("primaryTitle", "startYear", "numVotes", "averageRating").
            where((f.col("numVotes") > 100000) & (f.col("titleType") == "movie")).
            orderBy("averageRating", ascending=False).limit(10))

        return self.all_time_top_films

    def get_last_decade_top_films(self):
        """Returns last 10 years top 10 films"""
        self.last_decade_top_films = (
            self.title_basics.
            join(self.title_ratings, on='tconst', how='left').
            select("primaryTitle", "startYear", "numVotes", "averageRating").
            where((f.col("numVotes") > 100000) & (f.col("titleType") == "movie")
                  & (f.col("startYear") > 2022 - 10)).
            orderBy("averageRating", ascending=False).limit(10))
        return self.last_decade_top_films

    def get_sixtys_top_films(self):
        """Returns 60's top 10 films"""
        self.sixtys_top_films = (
            self.title_basics.
            join(self.title_ratings, on='tconst', how='left').
            select("primaryTitle", "startYear", "numVotes", "averageRating").
            where((f.col("numVotes") > 100000) & (f.col("titleType") == "movie") &
                  (1960 <= f.col("startYear")) & (f.col("startYear") < 1970)).
            orderBy("averageRating", ascending=False).limit(10))
        return self.sixtys_top_films

    def get_top_10_each_genre(self):
        """Returns top 10 films of each genre"""
        title_basics_genre = (
            self.title_basics.
            withColumn('split', f.split(f.col('genres'), ',')).
            withColumn('exploded', f.explode(f.col('split'))).
            drop('genres', 'split').
            withColumnRenamed('exploded', 'genre'))

        window = (Window.partitionBy(title_basics_genre['genre']).
                  orderBy(self.title_ratings['averageRating'].desc()))

        self.top_10_each_genre = (
            title_basics_genre.
            join(self.title_ratings, on='tconst', how='left').
            select("primaryTitle", "genre", "numVotes", "averageRating").
            where((f.col("numVotes") > 100000) & (f.col("titleType") == "movie")).
            withColumn('row', f.row_number().over(window)).where(f.col('row') <= 10))

        return self.top_10_each_genre

    def get_top_10_each_genre_decades(self):
        """Returns top films by genres for each decade """

        title_basics_genre = (
            self.title_basics.withColumn('split', f.split(f.col('genres'), ',')).
            withColumn('exploded', f.explode(f.col('split'))).drop('genres', 'split').
            withColumnRenamed('exploded', 'genre'))

        title_basics_genre = (title_basics_genre.
                              withColumn("decade", f.floor(f.col('startYear') / 10)))

        window = (Window.partitionBy(title_basics_genre['decade'],
                                     title_basics_genre['genre']).
                  orderBy(self.title_ratings['averageRating'].desc()))

        self.top_10_each_genre_decades = (
            title_basics_genre.
            join(self.title_ratings, on='tconst', how='left').
            select("primaryTitle", "genre", "numVotes", "averageRating", "decade").
            where((f.col("numVotes") > 100000) & (f.col("titleType") == "movie")
                  & (f.col("decade") >= 195)).
            withColumn('row', f.row_number().over(window)).where(f.col('row') <= 10).
            withColumn("yearRange", f.concat((f.col('decade') * 10).cast('string'),
                                             f.lit('-'),
                                             ((f.col('decade') + 1) * 10).cast(
                                                 'string'))).drop("decade"))
        return self.top_10_each_genre_decades

    def get_top_actors(self):
        """Actors (alive) that took roles in the top films"""

        top_films = (
            self.title_basics.
            join(self.title_ratings, on='tconst', how='left').
            select('tconst', "primarytitle", "startyear", "numVotes", "averageRating").
            where((f.col("numVotes") > 100000) & (f.col("titletype") == "movie")).
            orderBy("averageRating", ascending=False))

        actors = self.title_principals.where((f.col('category') == 'actor'))

        self.top_actors = (
            actors.join(self.name_basics, on='nconst').
            join(top_films, on='tconst').
            where(f.col('deathYear').isNull()).where(f.col('averageRating').isNotNull()).
            groupBy('primaryName').count().withColumnRenamed("count", "top_films").
            orderBy('top_films', ascending=False))

        return self.top_actors

    def get_top_5_of_director(self):
        """Returns top 5 films of each director"""

        title_crew_sp_directors = (
            self.title_crew.withColumn('split', f.split(f.col('directors'), ',')).
            withColumn('exploded', f.explode(f.col('split'))).
            drop('directors', 'split').
            withColumnRenamed('exploded', 'directors'))

        director_film = (
            title_crew_sp_directors.
            join(self.title_basics, on='tconst').
            join(self.name_basics,
                 title_crew_sp_directors.directors == self.name_basics.nconst).
            join(self.title_ratings, on="tconst").where(f.col("titletype") == "movie").
            select("tconst", "primaryName", "primarytitle", "averageRating"))

        window = (Window.partitionBy(director_film['primaryName']).
                  orderBy(self.title_ratings['averageRating'].desc()))

        self.top_5_of_director = (
            director_film.withColumn('rank', f.row_number().over(window)).
            where(f.col('rank') <= 5).withColumnRenamed("primaryName", "Director"))

        return self.top_5_of_director

def get_postgres(db_name):
    """Establishes postgres connection"""
    conn_string = f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}/{db_name}'
    db = create_engine(conn_string)
    conn = db.connect()
    return conn

def db_minio_to_postgres(database, table):
    conn = get_postgres(db_name="tmdb")
    database.to_sql(table, con=conn, if_exists='replace', index=False)


def get_statistics():
    film_schema = FilmDBSchemas()
    films = FilmDB(film_schema)

    # task 1
    db_minio_to_postgres(films.get_all_time_top_films().toPandas(),
                         'get_all_time_top_films')

    db_minio_to_postgres(films.get_last_decade_top_films().toPandas(),
                         'get_last_decade_top_films')

    db_minio_to_postgres(films.get_sixtys_top_films().toPandas(),
                         'get_sixtys_top_films')

    # task 2
    db_minio_to_postgres(films.get_top_10_each_genre().toPandas(),
                         'get_top_10_each_genre')

    # task 3
    db_minio_to_postgres(films.get_top_10_each_genre_decades().toPandas(),
                         'get_top_10_each_genre_decades')

    # task 4
    db_minio_to_postgres(films.get_top_actors().toPandas(),
                         'get_top_actors')

    # task 5
    db_minio_to_postgres(films.get_top_5_of_director().toPandas(),
                         'get_top_5_of_director')


with DAG(
        'get_statistics',
        description='processes dats creates tables with statistics on postgres',
        start_date=airflow.utils.dates.days_ago(14),
        schedule_interval=None,
        catchup=False,
        default_args=args) as dag:
    extract_data = PythonOperator(task_id="get_statistics", python_callable=get_statistics)
