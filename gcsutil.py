import gcsfs
import pickle
import pandas as pd
from google.cloud import storage

class GCSUtil:
    def __init__(self, bucket_name):
        self.client = storage.Client()
        self.bucket = self.client.get_bucket(bucket_name)
        self.bucket_name = bucket_name
        self.datapath = "gcs://{}/".format(bucket_name)
        self.fs = gcsfs.GCSFileSystem()
        
        
    def read_csv(self, path):
        return pd.read_csv(self.datapath + path)
        
        
    def save_df_as_csv(self, df, filepath):
        file_type = filepath.split(".")[-1]
        if file_type=="csv":
            self.bucket.blob(filepath).upload_from_string(df.to_csv(index=False), 'text/csv')
        else:
            print("Incorrect File Format. CSV expected, received '{}'".format(file_type))
            
            
    def save_to_gcs(self, data, path):
        with self.fs.open(self.bucket_name + path, 'wb') as f:
            f.write(pickle.dumps(data))
        
        
    ## Read object
    def read_from_gcs(self, path):
        with self.fs.open(self.bucket_name + path, 'rb') as f:
            obj = pickle.loads(f.read())
        return obj 