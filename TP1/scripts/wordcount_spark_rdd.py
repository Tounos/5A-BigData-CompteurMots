import sys
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("wordcount_rdd").master("local[*]").getOrCreate()

    compte = (
        spark.sparkContext.textFile(sys.argv[1])
        .flatMap(lambda ligne: ligne.split())       # map : ligne → mots
        .map(lambda mot: (mot, 1))                  #       mot → (mot, 1)
        .reduceByKey(lambda a, b: a + b)            # combine + shuffle + reduce
    )
    resultats = compte.collect()                    # action : le calcul démarre ici

    for mot, nb in sorted(resultats, key=lambda x: x[1], reverse=True):
        print(f"{mot} : {nb}")
    spark.stop()
