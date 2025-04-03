import sys
import boto3
import time
from awsglue.utils import getResolvedOptions

def run_redshift_query(query, cluster_id, database, db_user):
    """
    Run a Redshift query using the Redshift Data API and wait for completion
    """
    redshift_data = boto3.client('redshift-data')
    
    # Start query execution
    response = redshift_data.execute_statement(
        ClusterIdentifier=cluster_id,
        Database=database,
        DbUser=db_user,
        Sql=query
    )
    
    query_id = response['Id']
    print(f"Started Redshift query: {query_id}")
    print(f"Query text: {query}")
    
    # Wait for query to complete
    status = 'STARTED'
    while status in ['STARTED', 'SUBMITTED', 'PICKED']:
        response = redshift_data.describe_statement(Id=query_id)
        status = response['Status']
        print(f"Query status: {status}")
        if status in ['STARTED', 'SUBMITTED', 'PICKED']:
            time.sleep(5)
    
    # Check the final state
    if status == 'FINISHED':
        print(f"Query completed successfully!")
        return True
    else:
        error = response.get('Error', 'Unknown error')
        print(f"Query failed: {error}")
        return False

def main():
    # Get job parameters (added 'layer')
    args = getResolvedOptions(sys.argv, [
        'cluster_id', 
        'database', 
        'db_user',
        'layer'
    ])
    
    cluster_id = args['cluster_id']
    database = args['database']
    db_user = args['db_user']
    layer = args['layer']
    
    # Hardcoded list of raw queries
    raw_queries = [
        "COPY raws.apartments FROM 's3://****/raw/apartments_attributes.csv' IAM_ROLE 'arn:aws:iam::******:role/RedShiftS3Role' CSV IGNOREHEADER 1 REGION 'region';",
        "COPY raws.user_views FROM 's3://****/raw/user_viewings.csv' IAM_ROLE 'arn:aws:iam::******:role/RedShiftS3Role' CSV IGNOREHEADER 1 REGION 'region';",
        "COPY raws.bookings FROM 's3://****/raw/bookings.csv' IAM_ROLE 'arn:aws:iam::******:role/RedShiftS3Role' CSV IGNOREHEADER 1 REGION 'region';",
        "COPY raws.listings FROM 's3://****/raw/apartments.csv' IAM_ROLE 'arn:aws:iam::******:role/RedShiftS3Role' CSV IGNOREHEADER 1 REGION 'region';"
    ]
    
    # Hardcoded list of curated queries
    curated_queries = [
        "COPY curated.apartments FROM 's3://****/curated/apartment_attributes' IAM_ROLE 'arn:aws:iam::******:role/RedShiftS3Role' FORMAT AS PARQUET;",
        "COPY curated.user_views FROM 's3://****/curated/user_viewing' IAM_ROLE 'arn:aws:iam::******:role/RedShiftS3Role' FORMAT AS PARQUET;",
        "COPY curated.bookings FROM 's3://****/curated/bookings' IAM_ROLE 'arn:aws:iam::******:role/RedShiftS3Role' FORMAT AS PARQUET;",
        "COPY curated.listings FROM 's3://****/curated/listings' IAM_ROLE 'arn:aws:iam::******:role/RedShiftS3Role' FORMAT AS PARQUET;"
    ]
    
    # Decide which queries to run based on layer argument
    queries = raw_queries if layer == 'raw' else curated_queries
    
    print(f"Running {len(queries)} queries on Redshift cluster: {cluster_id}, database: {database}")
    
    # Execute each query sequentially
    for i, query in enumerate(queries):
        print(f"\n--- Running query {i+1}/{len(queries)} ---")
        success = run_redshift_query(query, cluster_id, database, db_user)
        if not success:
            print(f"Query {i+1} failed, but continuing with next query...")
    
    print("All queries processed.")

if __name__ == "__main__":
    main()