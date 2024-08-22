import pymysql
import io
import boto3
import os
from datetime import datetime

def lambda_handler(event, context):
    # Retrieve environment variables
    db_host = os.getenv('DB_HOST')
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')
    db_name = os.getenv('DB_NAME')
    s3_bucket = os.getenv('S3_BUCKET', 'test-rds-demo')  # Default bucket name if not set
    s3_key = f'backups/dump_{datetime.utcnow().strftime("%Y%m%d_%H%M%S")}.sql'

    # Create an in-memory file-like object to hold the SQL dump
    dump_buffer = io.StringIO()

    # Connect to the MySQL database
    connection = pymysql.connect(
        host=db_host,
        user=db_user,
        password=db_password,
        db=db_name
    )
    print("connection done....\n")
    try:
        with connection.cursor() as cursor:
            # Get the list of tables
            cursor.execute("SHOW TABLES")
            tables = cursor.fetchall()

            for table in tables:
                table_name = table[0]
                print("taking dmp.... \n")
                # Dump table structure
                cursor.execute(f"SHOW CREATE TABLE {table_name}")
                create_table_stmt = cursor.fetchone()[1]
                dump_buffer.write(f"-- Table structure for `{table_name}`\n")
                dump_buffer.write(f"{create_table_stmt};\n\n")
                print("table data dump....\n")
                # Dump table data
                cursor.execute(f"SELECT * FROM {table_name}")
                print("print 1")
                rows = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]

                for row in rows:
                    values = ', '.join(f"'{value}'" if value is not None else 'NULL' for value in row)
                    dump_buffer.write(f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({values});\n")

                dump_buffer.write("\n")
        print("uploading..........\n")
        print(s3_bucket)
        print("............................................\n")
        # Upload the SQL dump to S3
        s3_client = boto3.client('s3')
        s3_client.put_object(
            Bucket=s3_bucket,
            Key=s3_key,
            Body=dump_buffer.getvalue()
        )
        print("uploading done.......\n")
        return {
            'statusCode': 200,
            'body': f"Database dump created and uploaded to S3 successfully: s3://{s3_bucket}/{s3_key}"
        }

    except Exception as e:
        print(f"An error occurred: {e}")
        return {
            'statusCode': 500,
            'body': f"An error occurred: {e}"
        }

    finally:
        connection.close()
