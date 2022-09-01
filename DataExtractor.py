import pandas as pd
import numpy as np
from datetime import datetime
from io import StringIO  # python3; python2: BytesIO
import boto3


class DataExtractor:

    def __init__(self, df, search_key):
        self.filtered = None
        self.data = []
        self.df = df
        self.search_key = search_key
        self.read_tsv_file()

    def read_tsv_file(self):
        # self.df = pd.read_csv("data.tsv", sep='\t')
        self.filtered = self.df[self.df['event_list'] == 1]
        print(self.filtered.head())
        self.process_data_frames()

    def process_data_frames(self):
        for ind in self.filtered.index:
            search_engine = self.filtered['referrer'][ind]
            product = self.filtered['product_list'][ind].split(";")
            arr = [search_engine, product[1], int(product[3])]
            # print(arr)
            self.data.append(arr)
        print(self.data)

    def write_filtered_data(self):
        df = None
        try:
            self.data = np.array(self.data)
            # df = pd.DataFrame({
            #     "Search Engine": self.data[:, 0],
            #     "Search Key": self.data[:, 1],
            #     "Total Revenue": self.data[:, 2]
            # })
            df = pd.DataFrame(self.data, columns=['Search Engine', 'Search Key',
                                                  'TotalRevenue'])
            print(df.head())
            df = df.sort_values(by=['TotalRevenue'], ascending=False)
            name = datetime.now().date()
            print(name)
            # df.to_csv(str(name) + "_SearchKeywordPerformance.tsv", sep="\t")
            bucket = 'searchkeyworkbucket'  # already created on S3
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, sep="\t")
            print("csv buffer done")
            s3_resource = boto3.resource('s3')
            print("s3 resource created")
            s3_resource.Object(bucket, str(name) + "_SearchKeywordPerformance.tsv").put(Body=csv_buffer.getvalue())
            print("s3 resource witten")
        except Exception as e:
            # print(e)
            return df


if __name__ == "__main__":
    dx = DataExtractor("1")
    dx.write_filtered_data()
