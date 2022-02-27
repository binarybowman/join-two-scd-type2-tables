from pyspark.sql import SparkSession,DataFrame
from pyspark.sql.functions import col,regexp_replace,when,unix_timestamp,from_unixtime,min,udf,lit
from pyspark.sql.types import StringType,DateType,BooleanType,IntegerType
from datetime import date, datetime, timedelta,time

def joinTwoScdTables(df1, df2, df1Key, df2Key):
    today_midnight = datetime.combine(datetime.today(), time.min)
    tomorrow_midnight = today_midnight + timedelta(days=1)

    df1 = df1.withColumn('valid_to', regexp_replace('valid_to', '/', '-')).withColumn('valid_from',
                                                                                      regexp_replace('valid_from', '/',
                                                                                                     '-'))
    df1 = df1.withColumn('valid_to', df1['valid_to'].cast(DateType())).withColumn('valid_from',
                                                                                  df1['valid_from'].cast(DateType()))
    df1 = df1.withColumn('valid_to', when(col('valid_to').isNull(), tomorrow_midnight).otherwise(col('valid_to')))
    df1 = df1.withColumn('valid_to', unix_timestamp(df1.valid_to, format='yyyy-MM-dd')).withColumn('valid_from',
                                                                                                   unix_timestamp(
                                                                                                       df1.valid_from,
                                                                                                       format='yyyy-MM-dd'))

    df2 = df2.withColumn('valid_to', regexp_replace('valid_to', '/', '-')).withColumn('valid_from',
                                                                                      regexp_replace('valid_from', '/',
                                                                                                     '-'))
    df2 = df2.withColumn('valid_to', df2['valid_to'].cast(DateType())).withColumn('valid_from',
                                                                                  df2['valid_from'].cast(DateType()))
    df2 = df2.withColumn('valid_to', when(col('valid_to').isNull(), tomorrow_midnight).otherwise(col('valid_to')))
    df2 = df2.withColumn('valid_to', unix_timestamp(df2.valid_to, format='yyyy-MM-dd')).withColumn('valid_from',
                                                                                                   unix_timestamp(
                                                                                                       df2.valid_from,
                                                                                                       format='yyyy-MM-dd'))

    df1 = df1.withColumn("isActive", col("isActive").cast(BooleanType()))
    df2 = df2.withColumn("isActive", col("isActive").cast(BooleanType()))
    print("befor_join")
    result = df1.join(df2, on=(df1[df1Key] == df2[df2Key]) & \
                              (((df1.valid_from <= df2.valid_from) & (df1.valid_to > df2.valid_from)) | \
                               ((df1.valid_from < df2.valid_to) & (df1.valid_to > df2.valid_to)) | \
                               ((df1.valid_from >= df2.valid_from) & (df1.valid_to <= df2.valid_to)) | \
                               ((df2.valid_from >= df1.valid_from) & (df2.valid_to < df1.valid_to))), how="inner") \
        .drop(df1[df1Key])

    res = result.withColumn('final_valid_from',
                            when(df1.valid_from >= df2.valid_from, from_unixtime(df1.valid_from)).otherwise(
                                from_unixtime(df2.valid_from)))
    res = res.withColumn('final_valid_to', when(df1.valid_to >= df2.valid_to, from_unixtime(df2.valid_to)).otherwise(
        from_unixtime(df1.valid_to)))
    res = res.withColumn('final_isActive', when(df1.isActive.isNull(), df2.isActive) \
                         .otherwise(when(df2.isActive.isNull(), df1.isActive) \
                                    .otherwise(df1.isActive & df2.isActive)))
    df1_min = {}
    df2_min = {}
    df1_min_df = df1.groupBy(df1Key).agg(min("valid_from").alias('min_start_date'))
    df2_min_df = df2.groupBy(df2Key).agg(min("valid_from").alias('min_start_date'))

    for row in df1_min_df.rdd.collect():
        df1_min[row[df1Key]] = row.min_start_date
    for row in df2_min_df.rdd.collect():
        df2_min[row[df2Key]] = row.min_start_date

    def getMinStartDate(a, pk):
        if (a == 1):
            if pk in df1_min:
                return df1_min[pk]
            else:
                return 2147483647
        if a == 2:
            if pk in df2_min:
                return df2_min[pk]
            else:
                return 2147483647

    getMinStartDateUdf = udf(getMinStartDate, IntegerType())
    df1_past = df1.filter((df1.valid_from < getMinStartDateUdf(lit(2), df1[df1Key])) & (
                df1.valid_to < getMinStartDateUdf(lit(2), df1[df1Key])))
    df2_past = df2.filter((df2.valid_from < getMinStartDateUdf(lit(1), df2[df2Key])) & (
                df2.valid_to < getMinStartDateUdf(lit(1), df2[df2Key])))
    df1_past = df1_past.withColumn('final_valid_from', from_unixtime(df1.valid_from)).withColumn('final_valid_to',
                                                                                                 from_unixtime(
                                                                                                     df1.valid_to)).withColumn(
        'final_isActive', col('isActive'))
    df2_past = df2_past.withColumn('final_valid_from', from_unixtime(df2.valid_from)).withColumn('final_valid_to',
                                                                                                 from_unixtime(
                                                                                                     df2.valid_to)).withColumn(
        'final_isActive', col('isActive'))

    df1_first_intersect = df1.filter((df1.valid_from < getMinStartDateUdf(lit(2), df1[df1Key])) & (
                df1.valid_to > getMinStartDateUdf(lit(2), df1[df1Key])))
    df1_first_intersect = df1_first_intersect.withColumn('final_valid_from',
                                                         from_unixtime(df1_first_intersect.valid_from)).withColumn(
        'final_valid_to', from_unixtime(lit(getMinStartDateUdf(lit(2), df1_first_intersect[df1Key])))).withColumn(
        'final_isActive', col('isActive'))

    df2_first_intersect = df2.filter((df2.valid_from < getMinStartDateUdf(lit(1), df2[df2Key])) & (
                df2.valid_to > getMinStartDateUdf(lit(1), df2[df2Key])))
    df2_first_intersect = df2_first_intersect.withColumn('final_valid_from',
                                                         from_unixtime(df2_first_intersect.valid_from)).withColumn(
        'final_valid_to', from_unixtime(lit(getMinStartDateUdf(lit(1), df2_first_intersect[df2Key]
                                                               )))).withColumn('final_isActive', col('isActive'))

    res = res.drop(df1['valid_from']).drop(df1['valid_to']).drop(df2['valid_from']).drop(df2['valid_to']) \
        .drop(df2['isActive']).drop(df1['isActive'])

    df1_past = df1_past.drop(df1_past['valid_from']).drop(df1_past['valid_to']) \
        .drop(df1_past['isActive'])

    df2_past = df2_past.drop(df2_past['valid_from']).drop(df2_past['valid_to']) \
        .drop(df2_past['isActive'])
    df1_first_intersect = df1_first_intersect.drop(df1_first_intersect['valid_from']).drop(
        df1_first_intersect['valid_to']) \
        .drop(df1_first_intersect['isActive'])
    df2_first_intersect = df2_first_intersect.drop(df2_first_intersect['valid_from']).drop(
        df2_first_intersect['valid_to']) \
        .drop(df2_first_intersect['isActive'])

    print("before_union")
    final_df = res.unionByName(df1_past, allowMissingColumns=True) \
        .unionByName(df1_first_intersect, allowMissingColumns=True) \
        .unionByName(df2_past, allowMissingColumns=True) \
        .unionByName(df2_first_intersect, allowMissingColumns=True)
    # final_df =final_df.withColumn("final_isActive",when(final_df.final_valid_to.isNull(),"True"),
    #                               otherwise(final_df.final_isActive))
    print("after_union")
    s = str(tomorrow_midnight)
    df = final_df
    df = df.withColumn('final_valid_to', regexp_replace('final_valid_to', s, ''))
    df = df.withColumnRenamed('final_valid_to', 'valid_to').withColumnRenamed('final_valid_from',
                                                                              'valid_from').withColumnRenamed(
        'final_isActive', 'isActive')
    df = df.withColumn("isActive",
                       when(df.valid_to.isNull(), True).otherwise(when(df.valid_to == '', True).otherwise(False)))
    print("join completed")
    return df
