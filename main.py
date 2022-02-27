from pyspark.sql import SparkSession,DataFrame
from pyspark.sql.functions import col,regexp_replace,when,unix_timestamp,from_unixtime,min,udf,lit
from pyspark.sql.types import StringType,DateType,BooleanType,IntegerType
from datetime import date, datetime, timedelta,time
from util.scd_type_two_join import joinTwoScdTables


if __name__ == '__main__':
    spark = SparkSession.builder.master("local[1]").appName("practice").getOrCreate()
    emp_role = spark.createDataFrame(
        data=[["1", "yeshwanth", "Assosiate software engineer", '2019-7-15', "2021-4-1", False],
              ["1", "yeshwanth", "Software Engineer", '2021-4-1', None, True]
              ],
        schema=["emp_id", "emp_name", "emp_role", "valid_from", "valid_to", "isActive"])
    emp_role = emp_role.withColumn("valid_from", col('valid_from').cast(DateType())). \
        withColumn("valid_to", col("valid_to").cast(DateType()))
    emp_role.show()
    emp_project = spark.createDataFrame(data=[["1", "XYZ", '2019-7-15', "2020-3-1", False],
                                              ["1", "ABC", '2021-3-1', "2022-1-1", False],
                                              ["1", "MNOP", '2022-1-1', None, True],
                                              ],
                                        schema=["emp_id", "project_name", "valid_from", "valid_to", "isActive"])
    emp_project = emp_project.withColumn("valid_from", col('valid_from').cast(DateType())). \
        withColumn("valid_to", col("valid_to").cast(DateType()))
    emp_project.show()
    joinedDF = joinTwoScdTables(emp_project, emp_role, "emp_id", "emp_id")
    joinedDF.show()
