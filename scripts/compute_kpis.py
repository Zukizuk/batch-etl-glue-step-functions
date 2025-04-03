import sys
import boto3
import time
from awsglue.utils import getResolvedOptions

def run_redshift_query(query, cluster_id, database, db_user):
    """
    Run a Redshift query and wait for completion
    """
    redshift_data = boto3.client('redshift-data')
    response = redshift_data.execute_statement(
        ClusterIdentifier=cluster_id,
        Database=database,
        DbUser=db_user,
        Sql=query
    )
    query_id = response['Id']
    print(f"Started query: {query_id} | Query: {query[:50]}...")
    
    status = 'STARTED'
    while status in ['STARTED', 'SUBMITTED', 'PICKED']:
        response = redshift_data.describe_statement(Id=query_id)
        status = response['Status']
        print(f"Status: {status}")
        if status in ['STARTED', 'SUBMITTED', 'PICKED']:
            time.sleep(5)
    
    if status == 'FINISHED':
        print("Query completed successfully!")
        return True
    else:
        error = response.get('Error', 'Unknown error')
        print(f"Query failed: {error}")
        return False

def main():
    # Get job parameters
    args = getResolvedOptions(sys.argv, ['cluster_id', 'database', 'db_user'])
    
    cluster_id = args['cluster_id']
    database = args['database']
    db_user = args['db_user']
    
    # Read queries from S3
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket="bucket", Key="queries/compute_load_kpis.sql")
    query_content = response['Body'].read().decode('utf-8')
    
    # Split queries by semicolon and filter out empty lines
    queries = [q.strip() for q in query_content.split(';') if q.strip()]
    
    print(f"Running {len(queries)} queries on Redshift cluster: {cluster_id}, database: {database}")
    
    # Execute each query sequentially
    for i, query in enumerate(queries):
        print(f"\n--- Running query {i+1}/{len(queries)} ---")
        success = run_redshift_query(query, cluster_id, database, db_user)
        if not success:
            print(f"Query {i+1} failed, but continuing...")

    print("All queries processed.")

if __name__ == "__main__":
    main()