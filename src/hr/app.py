from pyspark.sql.functions import lit, concat_ws, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from src.hr import utils


def run(app):
    data = [('James', 'Smith', 'M', 3000),
            ('Anna', 'Rose', 'F', 4100),
            ('Robert', 'Williams', 'M', 6200)]

    schema = StructType([
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("salary", IntegerType(), True)
        ])

    # columns = ["first_name", "last_name", "gender", "salary"]
    df = app.createDataFrame(data=data, schema=schema)

    # Add column from existing column
    df = df.withColumn("bonus_amount", df.salary * 0.3)

    # Add column by concatenating existing columns
    df = df.withColumn("name", concat_ws(",", "first_name", 'last_name'))

    # Add column by calculating from existing columns
    df = (df.withColumn("grade", when((df.salary < 4000), lit("A"))
                        .when((df.salary >= 4000) & (df.salary <= 5000), lit("B"))
                        .otherwise(lit("C"))))

    df.show()

    app.stop()


if __name__ == "__main__":
    """
        Usage: hr.
    """
    spark = utils.get_spark_context("HR Spark app - calculates grade and bonus information.")

    run(spark)
