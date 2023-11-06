"""
Main cli or app entry point
"""

from mylib.lib import extract, transform, load, describe, query
from pyspark.sql import SparkSession

def main():
    file = extract()
    transform(file)
    spark = SparkSession.builder.appName("Survival").getOrCreate()
    df = load(spark)
    # print(df)
    describe(df)
    query(df, "ttmp")
    spark.stop()
    return 0
    # quit()

if __name__ == "__main__":
    main()
