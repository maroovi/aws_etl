import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

def read_nested_json(df):
    column_list = []
    for column_name in df.schema.names:
        if isinstance(df.schema[column_name].dataType, ArrayType):
            df = df.withColumn(column_name,explode(column_name))
            column_list.append(column_name)
        elif isinstance(df.schema[column_name].dataType, StructType):
            for field in df.schema[column_name].dataType.fields:
                column_list.append(col(column_name + "." + field.name).alias(column_name + "_" + field.name))
        else:
            column_list.append(column_name)
    df = df.select(column_list)
    return df
    
def flatten(df):
  read_nested_json_flag = True
  while read_nested_json_flag:
    df = read_nested_json(df);
    read_nested_json_flag = False
    for column_name in df.schema.names:
      if isinstance(df.schema[column_name].dataType, ArrayType):
        read_nested_json_flag = True
      elif isinstance(df.schema[column_name].dataType, StructType):
        read_nested_json_flag = True;
  return df;

def main():
    ## @params: [JOB_NAME]
    args = getResolvedOptions(sys.argv, ["VAL1","VAL2"])
    file_name=args['VAL1']
    bucket_name=args['VAL2']
    print("Bucket Name" , bucket_name)
    print("File Name" , file_name)
    input_file_path="s3a://{}/{}".format(bucket_name,file_name)
    print("Input File Path : ",input_file_path);
    
    df = spark.read.option("multiline", True).option("inferSchema", False).json(input_file_path)
    df1=flatten(df)
    df1.coalesce(1).write.format("csv").option("header", "true").save("s3a://destinationflattenjson/{}".format(file_name.split('.')[0]))

main()



