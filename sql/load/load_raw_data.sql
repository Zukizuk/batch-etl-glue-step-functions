-- Load apartments.csv
COPY raws.apartments
FROM 's3://batch-data-bucket-zuki/raw/apartments_attributes.csv'
IAM_ROLE 'arn:aws:iam::288761743948:role/RedShiftS3Role' 
CSV
IGNOREHEADER 1
REGION 'eu-west-1';

-- Load user_views.csv
COPY raws.user_views
FROM 's3://batch-data-bucket-zuki/raw/user_viewings.csv'
IAM_ROLE 'arn:aws:iam::288761743948:role/RedShiftS3Role' 
CSV
IGNOREHEADER 1
REGION 'eu-west-1';

-- Load bookings.csv
COPY raws.bookings
FROM 's3://batch-data-bucket-zuki/raw/bookings.csv'
IAM_ROLE 'arn:aws:iam::288761743948:role/RedShiftS3Role' 
CSV
IGNOREHEADER 1
REGION 'eu-west-1';

-- Load listings.csv
COPY raws.listings
FROM 's3://batch-data-bucket-zuki/raw/apartments.csv'
IAM_ROLE 'arn:aws:iam::288761743948:role/RedShiftS3Role' 
CSV
IGNOREHEADER 1
REGION 'eu-west-1';