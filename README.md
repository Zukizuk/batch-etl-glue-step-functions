# Batch Data Processing for Rental Marketplace Analytics

## Project Overview

This project implements a batch data processing pipeline for a rental marketplace platform, similar to Airbnb. The goal is to extract, transform, and load (ETL) rental listing and user interaction data into Amazon Redshift to support business intelligence and analytical reporting.

## Features

- **Data Extraction:** Pulls rental listing data from AWS Aurora MySQL and stores it in Amazon S3.
- **Data Loading:** Moves raw data from S3 into Amazon Redshift for structured processing.
- **Multi-Layer Architecture:** Implements Raw, Curated, and Presentation layers in Redshift.
- **ETL Processing:** Uses AWS Glue for data transformation and processing.
- **Orchestration:** Manages workflow execution using AWS Step Functions.
- **Key Insights Generation:** Computes rental performance and user engagement metrics.

## Data Pipeline Workflow

The batch data pipeline consists of the following steps:

1. **ExtractRawToS3** - Extracts raw data from AWS Aurora MySQL and loads it into Amazon S3.
2. **CopyRawToRedshift** - Moves raw data from S3 to Redshift's Raw layer.
3. **TransformRawToS3** - Cleans and transforms the raw data, storing it back in S3.
4. **CopyCuratedToRedshift** - Moves transformed data to Redshift’s Curated layer.
5. **ExecuteComputeQueries** - Runs analytical queries to generate business metrics.

## Key Business Metrics

### Rental Performance Metrics

- **Average Listing Price:** Computes the average price of active rental listings each week.
- **Occupancy Rate:** Measures the percentage of available rental nights that were booked over a month.
- **Most Popular Locations:** Identifies the most frequently booked cities every week.
- **Top Performing Listings:** Tracks properties with the highest confirmed revenue per week.

### User Engagement Metrics

- **Total Bookings per User:** Counts the total number of rentals booked per user weekly.
- **Average Booking Duration:** Computes the mean duration of confirmed stays over time.
- **Repeat Customer Rate:** Measures the percentage of users who book more than once within a rolling 30-day period.

## Project Structure

```
├── assets/
│   ├── images/           # Screenshots and visual assets
├── script/               # Python scripts for ETL processing
├── sql/                  # SQL queries for Redshift transformations
├── step_function_code.json  # AWS Step Function definition
├── README.md             # Project documentation
```

## Setup & Installation

1. **Prerequisites:** Ensure AWS services (Glue, Redshift, S3, Step Functions) are configured.
2. **Clone Repository:**
   ```sh
   git clone <repo_url>
   cd <repo_name>
   ```
3. **Deploy AWS Glue Jobs:** Upload ETL scripts and set up AWS Glue jobs.
4. **Configure Step Function:** Deploy the `step_function_code.json` to AWS Step Functions.
5. **Run Pipeline:** Execute the Step Function to process data end-to-end.

## Evaluation Criteria

- Efficient implementation using AWS Glue & Step Functions.
- Correct data validation and transformation logic.
- Optimized Redshift schema for analytical queries.
- Well-documented setup and troubleshooting guide.

## Contributing

Contributions are welcome! Please submit a pull request with detailed explanations for changes.

## License

This project is licensed under the MIT License.

---

For more details, refer to the project documentation in docs/Technical Solution Document.
