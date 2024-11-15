from pyspark.sql import SparkSession
import os
import requests


def download_file(url, local_path):
    """Descargar un archivo desde una URL y guardarlo localmente."""
    try:
        print(f"Descargando {url}...")
        response = requests.get(url)
        response.raise_for_status() 
        with open(local_path, 'wb') as f:
            f.write(response.content)
        print("Descarga completada.")
    except Exception as e:
        print(f"Error al descargar el archivo: {e}")


def main():
    
    spark = SparkSession.builder \
        .appName("MovieLensDATtoParquet") \
        .getOrCreate()

    
    movies_dat_path = "/app/movies.dat"
    ratings_dat_path = "/app/ratings.dat"
    users_dat_path = "/app/users.dat"

    
    movies_dat_url = "https://tecmovielens.s3.us-east-1.amazonaws.com/ml-1m/movies.dat"
    ratings_dat_url = "https://tecmovielens.s3.us-east-1.amazonaws.com/ml-1m/ratings.dat"
    users_dat_url = "https://tecmovielens.s3.us-east-1.amazonaws.com/ml-1m/users.dat"

    
    if not os.path.exists(movies_dat_path):
        download_file(movies_dat_url, movies_dat_path)

    if not os.path.exists(ratings_dat_path):
        download_file(ratings_dat_url, ratings_dat_path)

    if not os.path.exists(users_dat_path):
        download_file(users_dat_url, users_dat_path)

    
    print("Leyendo el archivo de películas...")
    movies_df = spark.read.csv(movies_dat_path, sep="::", header=False, inferSchema=True)
    movies_df = movies_df.toDF("movieId", "title", "genres")

    print("Leyendo el archivo de ratings...")
    ratings_df = spark.read.csv(ratings_dat_path, sep="::", header=False, inferSchema=True)
    ratings_df = ratings_df.toDF("userId", "movieId", "rating", "timestamp")

    print("Leyendo el archivo de usuarios...")
    users_df = spark.read.csv(users_dat_path, sep="::", header=False, inferSchema=True)
    users_df = users_df.toDF("userId", "gender", "age", "occupation", "zipcode")

    
    print("Convirtiendo y guardando los archivos en formato Parquet...")
    movies_df.write.parquet("/app/movies_parquet")
    ratings_df.write.parquet("/app/ratings_parquet")
    users_df.write.parquet("/app/users_parquet")

    print("Conversión completada.")
    spark.stop()


if __name__ == "__main__":
    main()
