
import os
import requests
import pandas
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


def extract(
    url: "str" = """https://raw.githubusercontent.com/nogibjj/Individual_Proj_2_Gavin_Li/main/resources/train.csv""",
    file_path = "resources/train.csv",
    directory = "resources"
):
    if not os.path.exists(directory):
        os.makedirs(directory)
    with requests.get(url) as r:
        with open(file_path, "wb") as f:
            f.write(r.content)
    return file_path

def transform(dataset: "str"):
    df = pandas.read_csv(dataset)
    df = df[["PassengerId", "Survived", "Pclass", "Sex"]]
    # print(df)
    df.to_csv(dataset, index=False)

def load(session, dataset: "str" = "resources/train.csv"):
    schema = StructType([
        StructField("PassengerId", IntegerType()),
        StructField("Survived", IntegerType()),
        StructField("Pclass", IntegerType()),
        StructField("Sex", StringType()),
    ])
    df = session.read.option("header", "true").schema(schema).csv(dataset)
    return df

def describe(df):
    return df.describe().show()

def query(df, name):
    spark = SparkSession.builder.appName("Survival").getOrCreate()
    df = df.createOrReplaceTempView(name)
    rslt = spark.sql("""
SELECT
    Sex, AVG(Survived) AS survival_rate
FROM ttmp
GROUP BY Sex
""")
    return rslt.show()
