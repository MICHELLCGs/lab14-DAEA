from pyspark.sql import SparkSession
import time


def main():
    
    spark = SparkSession.builder \
        .appName("MovieLensReadParquet") \
        .getOrCreate()

    
    start_time = time.time()

    
    print("Leyendo el archivo Parquet de películas...")
    movies_df = spark.read.parquet("/app/movies_parquet")

    print("Leyendo el archivo Parquet de ratings...")
    ratings_df = spark.read.parquet("/app/ratings_parquet")

    print("Leyendo el archivo Parquet de usuarios...")
    users_df = spark.read.parquet("/app/users_parquet")

  
    print("Contenido del DataFrame de películas:")
    movies_df.show()

    print("Contenido del DataFrame de ratings:")
    ratings_df.show()

    print("Contenido del DataFrame de usuarios:")
    users_df.show()

  
    end_time = time.time()
    elapsed_time = end_time - start_time

    
    print("|" + "-" * 67 + "|")
    print(f"| Tiempo total de ejecución: {elapsed_time:.2f} segundos |")
    print("|" + "-" * 67 + "|")


    spark.stop()


if __name__ == "__main__":
    main()
