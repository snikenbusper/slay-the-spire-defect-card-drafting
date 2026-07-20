import sys
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import BooleanType, LongType, IntegerType, DoubleType, \
StringType, ArrayType
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, coalesce, lit, array

sc = SparkContext.getOrCreate()
spark = SparkSession.builder.getOrCreate()

# Script generated for node S3DataSource
S3DataSource_1768541802259 = spark.read.format("json") \
    .option("multiLine", "true") \
    .load("S3_BUCKET_SOURCE_DIRECTORY")

def fn_1768624397071(input_df) -> DataFrame:
        defect_runs = input_df.select("event.*")\
        .filter(col("character_chosen") == "DEFECT")
        return defect_runs
# Script generated for node CustomCodeTransform
CustomCodeTransform_1768624397071 = S3DataSource_1768541802259.transform(fn_1768624397071)

def fn_1768624508312(input_df) -> DataFrame:
        EXCLUDE = ["neow_bonus", "neow_cost", "seed_played", "local_time"]
        def coalesce_by_type(df):
            for field in df.schema.fields:
                name = field.name
                dtype = field.dataType
                if name in EXCLUDE:
                    continue
                
                if str(dtype).startswith("StructType"):
                    continue
                if isinstance(dtype, BooleanType):
                    df = df.withColumn(name, coalesce(col(name), lit(False)))
                elif isinstance(dtype, (LongType, IntegerType)):
                    df = df.withColumn(name, coalesce(col(name), lit(0)))
                elif isinstance(dtype, DoubleType):
                    df = df.withColumn(name, coalesce(col(name), lit(0.0)))
                elif isinstance(dtype, StringType):
                    df = df.withColumn(name, coalesce(col(name), lit("None")))
                elif isinstance(dtype, ArrayType):
                    df = df.withColumn(name, coalesce(col(name), array()))
                else:
                    continue
            return df
        defect_runs = coalesce_by_type(input_df)
        defect_runs = defect_runs\
        .filter("is_beta = FALSE")\
        .filter("is_daily = FALSE")\
        .filter("is_endless = FALSE")\
        .filter("chose_seed = FALSE")\
        .filter("special_seed = 0")
        defect_runs = defect_runs.filter(col("ascension_level")==20)
        defect_runs = defect_runs.drop("is_beta", "is_daily", "is_endless", \
        "chose_seed", "special_seed", "ascension_level")
        return defect_runs
# Script generated for node CustomCodeTransform
CustomCodeTransform_1768624508312 = CustomCodeTransform_1768624397071.transform(fn_1768624508312)

# Script generated for node S3DataSink
CustomCodeTransform_1768624508312.write.format("parquet") \
    .partitionBy("victory") \
    .mode("overwrite") \
    .save("S3_BUCKET_TARGET_DIRECTORY")
