import json
import boto3
import pandas as pd
from DataExtractor import DataExtractor


def lambda_handler(event, context):
    df = read_from_s3bucket()
    if df is not None:
        de = DataExtractor(df, 1)
        re = de.write_filtered_data()
        print(re)
        return {"status": 200}
    print("actual execution starts")
    return {"status": 500}


def read_from_s3bucket():
    try:
        s3 = boto3.client('s3')
        data = s3.get_object(Bucket='searchkeyworkbucket', Key='data.tsv')
        print("Reading Data from s3...")
        contents = data['Body']
        print(data.get("Body"))

        df = pd.read_csv(contents, sep="\t")
        print(df.head())

        return df
    except Exception as e:
        print(e)