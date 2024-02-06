from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, when, last, split
from pyspark.sql.types import StringType, DoubleType
from pyspark.sql.window import Window

COLUMNS_EXPORT = ["index", "kraj_produkt",
                  "1_quantity", "1_weight", "1_value", "1_dynamic",
                  "2_quantity", "2_weight", "2_value", "2_dynamic",
                  "3_quantity", "3_weight", "3_value", "3_dynamic",
                  "4_quantity", "4_weight", "4_value", "4_dynamic",
                  "5_quantity", "5_weight", "5_value", "5_dynamic",
                  "6_quantity", "6_weight", "6_value", "6_dynamic",
                  "7_quantity", "7_weight", "7_value", "7_dynamic",
                  "8_quantity", "8_weight", "8_value", "8_dynamic",
                  "9_quantity", "9_weight", "9_value", "9_dynamic",
                  "total_quantity", "total_weight", "total_value", "total_dynamic"
                  ]

COLUMNS_INDEX = ['Country', 'Exports_2023Q1', 'Exports_2023Q2', 'Exports_2023Q3', 'Exports_2023Q4', 'GDP_2023Q1',
                 'GDP_2023Q2', 'GDP_2023Q3', 'GDP_2023Q4', 'Imports_2023Q1', 'Imports_2023Q2', 'Imports_2023Q3',
                 'Imports_2023Q4',
                 'Industrial_Production_2023Q1', 'Industrial_Production_2023Q2', 'Industrial_Production_2023Q3',
                 'Industrial_Production_2023Q4',
                 'Unemployment_rate_2023Q1', 'Unemployment_rate_2023Q2', 'Unemployment_rate_2023Q3',
                 'Unemployment_rate_2023Q4']


def type_of_text(x):
    if x.split("-")[0].strip().isdigit():
        return "kod"
    elif set(x.split("-")[0].strip()) <= set("IVX"):
        return "sekcja"
    else:
        return "kraj"


def transform_export_data(spark):
    df = spark.read.csv('hdfs://localhost:8020/user/projekt/obroty.csv')
    df = df.toDF(*COLUMNS_EXPORT)
    df = df.filter(df.index >= 3)

    type_of_text_UDF = udf(lambda x: type_of_text(x), StringType())
    df = df.withColumn("typ", type_of_text_UDF(df['kraj_produkt']))

    df = df.withColumn("kraj", when(df["typ"] != "kraj", None).otherwise(df["kraj_produkt"]))

    df = df.withColumn("index", df['index'].cast('int'))
    window = (
        Window
        .partitionBy()
        .orderBy('index')
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    df = df.withColumn('kraj', last('kraj', ignorenulls=True).over(window))



    df = df.filter(df.typ != 'kraj') \
        .withColumn("kod", split(df["kraj_produkt"], " - ").getItem(0)) \
        .withColumn("opis", split(df["kraj_produkt"], " - ").getItem(1))

    df = df.withColumn("subkod", when(df["typ"] != "sekcja", None).otherwise(df["kod"]))
    window = (
        Window
        .partitionBy()
        .orderBy('index')
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    df = df.withColumn('subkod', last('subkod', ignorenulls=True).over(window)) \
        .drop("index", "kraj_produkt")

    df.write.mode('overwrite').parquet("hdfs://localhost:8020/user/projekt/obroty_transform.csv")


def transform_index_data(spark):
    df = spark.read.option("infereSchema", "true").option('header', 'true').csv(
        'hdfs://localhost:8020/user/projekt/wskazniki.csv')
    df = df \
        .withColumn("2023Q1", df["2023Q1 [2023Q1]"].cast(DoubleType()).alias("2023Q1")) \
        .withColumn("2023Q2", df["2023Q2 [2023Q2]"].cast(DoubleType()).alias("2023Q2")) \
        .withColumn("2023Q3", df["2023Q3 [2023Q3]"].cast(DoubleType()).alias("2023Q3")) \
        .withColumn("2023Q4", df["2023Q4 [2023Q4]"].cast(DoubleType()).alias("2023Q4")) \
        .na.fill(0)

    df = df.groupby('Country').pivot('series').sum('2023Q1', '2023Q2', '2023Q3', '2023Q4')
    df = df.drop('null_sum(2023Q1)', 'null_sum(2023Q2)', 'null_sum(2023Q3)', 'null_sum(2023Q4)')

    df = df.toDF(*COLUMNS_INDEX)
    df.write.mode('overwrite').parquet("hdfs://localhost:8020/user/projekt/wskazniki_transform.csv")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("Projekt").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    transform_export_data(spark)
    transform_index_data(spark)
