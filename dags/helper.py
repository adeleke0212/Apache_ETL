from pymongo import MongoClient
from configparser import ConfigParser
import boto3
import pandas as pd

config = ConfigParser()
config.read('.env')

SECRET_KEY = config['S3_CONN']['secret_key']
ACCESS_KEY = config['S3_CONN']['access_key']
REGION = config['S3_CONN']['region']
BUCKET_NAME = config['S3_CONN']['bucket_name']

raw_test_path = "s3//{}/raw/{}.csv"
# raw_test_path = s3//bucket_name/raw/filename.csv


def create_s3_bucket():
    try:
        client = boto3.client(
            's3',
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_KEY

        )
        client.create_bucket(
            Bucket=BUCKET_NAME,
            CreateBucketConfiguration={
                'LocationConstraint': REGION
            }

        )
        print('Bucket created or already created')
    except Exception as error:
        print('Unable to create bucket')


# ============== EXTRACTION - LOADING

def save_to_lake(df, file_name):
    df.to_csv(
        raw_test_path.format(BUCKET_NAME, file_name), index=False, storage_options={
            'key': ACCESS_KEY, 'secret': SECRET_KEY
        }
    )


# connect to s3


# Create connection to mongodb database


def create_db_client():
    mongo_conn_string = 'mongodb+srv://student1:6u0N1fZ7cCd7j5uK@tenalytics.vecttgs.mongodb.net/?retryWrites=true&w=majority&appName=AtlasApp'
    conn = MongoClient(mongo_conn_string)
    return conn

# extract docs from mongo db

# extract merchants


def extract_from_merchants():
    db_client_conn = create_db_client()
    ordernow = db_client_conn.ordernow
    merchants = ordernow.merchants
    # merchants = db_client_conn.ordernow.merchants
    # a cursor equivalent of cursor.fetchall()
    return merchants.find()


# extract customers


def extract_from_customers():
    db_client_conn = create_db_client()
    customers = db_client_conn.ordernow.customers
    return customers

# extract orders


def extract_from_orders():
    client_conn = create_db_client()
    ordernow = client_conn.ordernow
    orders = ordernow.orders
    return orders

# Reading from the data lake


def read_csv_from_lake(s3_path):
    df = pd.read_csv(s3_path, storage_options={
        'key': ACCESS_KEY, 'secret': SECRET_KEY
    })
    print(df.head())
    return df

# Writing to data lake after transformation


def write_csv_to_lake(s3_path, df):
    df.to_csv(s3_path, storage_options={
        'key': ACCESS_KEY, 'secret': SECRET_KEY
    })
    print(df.head())
