import pyspark

from pyspark.context import SparkContext
from pyspark.sql.types import IntegerType, DateType
from pyspark.sql.functions import udf


def change_data_types(df_sql):
    df_sql = df_sql.withColumn("size", df_sql["size"].cast(IntegerType()))
    df_sql = df_sql.withColumn("date", df_sql["date"].cast(DateType()))
    print(df_sql.dtypes)


def rename_column(df_sql):
    df_sql = df_sql.withColumnRenamed("size", "new_size")
    print(df_sql.columns)


def sort_by_column(df_sql):
    df_sql.sort(df_sql.size.asc()).show(10)
    df_sql.sort(df_sql.size.desc()).show(10)


@udf
def calculate_percent(x):
    return str((round(x * 100, 3))) + "%"


def aggregation(df_sql):
    count = df_sql.filter(df_sql.size < 1000).count()
    # print(count)
    package_count = df_sql.groupBy("package").count().sort("count", ascending=False)
    # package_count.show(10)
    package_count = package_count.withColumn(
        "percent", calculate_percent(package_count["count"] / df_sql.count())
    )
    package_count.show()


def spark_sql(df_sql):
    package_count = df_sql.groupBy("package").count().sort("count", ascending=False)
    package_count.createOrReplaceTempView("package_count")
    result = sqlContext.sql("select * package_count as p")
    result.show()


if __name__ == "__main__":
    sc = SparkContext("local", "test")
    sqlContext = pyspark.SQLContext(sc)

    raw = sc.textFile("spark-practice/sample_data/2015-12-12.csv")
    cleaned = raw.map(lambda x: x.replace('"', "")).map(lambda x: x.split(","))
    df_sql = sqlContext.createDataFrame(
        schema=cleaned.filter(lambda x: x[0] == "date").collect()[0],
        data=cleaned.filter(lambda x: x[0] != "date"),
    )
    df_sql.persist()
    df_sql.show()
    # print(df_sql.columns)
    # print(df_sql.dtypes)

    # change_data_types(df_sql)
    # rename_column(df_sql)
    # sort_by_column(df_sql)
    # aggregation(df_sql)
    spark_sql(df_sql)