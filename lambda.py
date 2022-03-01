
import json
import boto3


def lambda_handler(event, context):
    file_name = event['Records'][0]['s3']['object']['key']
    bucketName=event['Records'][0]['s3']['bucket']['name']
    print("File Name : ",file_name)
    print("Bucket Name : ",bucketName)
    glue=boto3.client('glue');
    response = glue.start_job_run(JobName = "s3_lambda_glue_s3", Arguments={"--VAL1":file_name,"--VAL2":bucketName})
    print("Lambda Invoke ")

