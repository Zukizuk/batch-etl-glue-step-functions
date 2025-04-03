import sys
import boto3
import pymysql
import csv
from io import StringIO

# AWS & Database Configs
S3_BUCKET = "****"
S3_PREFIX = "raw/"  # Base Raw Layer path

DB_HOST = ""
DB_PORT = 3306
DB_NAME = ""  # Your database name
DB_USER = ""
DB_PASSWORD = ""

TABLES = ["apartments", "apartments_attributes", "bookings", "user_viewings"]  # Tables to extract

# AWS S3 Client
s3_client = boto3.client("s3")

def extract_and_upload_table(table_name):
    """
    Extracts data from MySQL table and uploads it as a CSV file to S3.
    """
    try:
        # Connect to Aurora MySQL
        conn = pymysql.connect(
            host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME, port=DB_PORT
        )
        cursor = conn.cursor()

        # Query Data
        query = f"SELECT * FROM {table_name}"
        cursor.execute(query)
        rows = cursor.fetchall()

        # Get column names
        column_names = [desc[0] for desc in cursor.description]

        # Convert result to CSV format
        csv_buffer = StringIO()
        csv_writer = csv.writer(csv_buffer)
        csv_writer.writerow(column_names)  # Write header
        csv_writer.writerows(rows)  # Write rows

        # Upload CSV to S3
        s3_key = f"{S3_PREFIX}{table_name}.csv"
        s3_client.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=csv_buffer.getvalue())

        print(f"✅ Extracted {table_name} and stored in S3: s3://{S3_BUCKET}/{s3_key}")

        # Close connection
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"❌ Failed to extract {table_name}: {e}")

# Loop through all tables and extract data
for table in TABLES:
    extract_and_upload_table(table)

