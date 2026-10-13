import sys
from pyspark.sql import SparkSession, functions as F

if __name__ == "__main__":
    spark = SparkSession.builder.appName("wordcount_df").master("local[*]").getOrCreate()

    compte = (
        spark.read.text(sys.argv[1])                                    # une colonne "value" par ligne
        .select(F.explode(F.split(F.trim("value"), r"\s+")).alias("mot"))
        .where(F.col("mot") != "")
        .groupBy("mot")
        .count()
        .orderBy(F.desc("count"))
    )
    resultats = compte.collect()                                        # action : le calcul démarre ici

    for ligne in resultats:
        print(f"{ligne['mot']} : {ligne['count']}")
    spark.stop()
