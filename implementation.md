# Implementation in AWS 

## S3 Bucket

Bucket: gaming-data-wztt7x

Folders: 
- api-holiday-data (contains public holidays from US, UK, Germany)
- raw-gaming-data (contains generated raw data)
- results (used to store results of queries)

## Lambda

fetch-holiday-wztt7x

Code:
```
import json
import csv
import io
import boto3
import urllib3

# Initialize S3 client and HTTP manager
s3_client = boto3.client('s3')
http = urllib3.PoolManager()

def lambda_handler(event, context):
    # --- Configuration ---
    # Ensure this is just the BUCKET name, not the path
    BUCKET_NAME = 'gaming-data-wztt7x' 
    YEAR = '2025'
    COUNTRIES = ['US', 'GB', 'DE']
    
    combined_data = []

    try:
        for country_code in COUNTRIES:
            url = f"https://date.nager.at/api/v3/PublicHolidays/{YEAR}/{country_code}"
            response = http.request('GET', url)
            
            if response.status == 200:
                holiday_list = json.loads(response.data.decode('utf-8'))
                combined_data.extend(holiday_list)
                print(f"Successfully fetched {len(holiday_list)} holidays for {country_code}")
            else:
                print(f"Failed to fetch data for {country_code}. Status: {response.status}")

        if not combined_data:
            return {'statusCode': 400, 'body': 'No data fetched'}

        # --- Convert JSON to CSV ---
        # Create an in-memory string buffer
        csv_buffer = io.StringIO()
        
        # Define the headers based on the keys in the first dictionary
        # This usually includes: date, localName, name, countryCode, fixed, global, types
        headers = combined_data[0].keys()
        
        writer = csv.DictWriter(csv_buffer, fieldnames=headers)
        writer.writeheader()
        writer.writerows(combined_data)

        # --- Save CSV to S3 ---
        # Storing in the 'api-holiday-data' folder as specified in your setup
        file_key = f"api-holiday-data/holidays_{YEAR}.csv"
        
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=file_key,
            Body=csv_buffer.getvalue(),
            ContentType='text/csv'
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps(f"Successfully uploaded CSV holiday data to {file_key}")
        }

    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error processing holiday data: {str(e)}")
        }
```

2025 holidays are saved in csv format gaming-data-wztt7x/api-holiday-data/holidays_2025

## Glue

Crawler name: gaming-crawler-wztt7x

Database: gamingdb

Tables: 
- api_holiday_data
- raw_gaming_data

## Athena
```
SELECT 
    g.region,
    g.game,
    g.date,
    g.hours_played,
    h.name AS holiday_name,
    -- Label days as Holiday or Normal for comparison
    CASE WHEN h.name IS NOT NULL THEN 'Holiday' ELSE 'Normal Day' END AS day_type
FROM "gamingdb"."raw_gaming_data" g
LEFT JOIN "gamingdb"."api_holiday_data" h 
    ON g.date = h.date AND g.region = h.countryCode
```

saved table as: merged_gaming_x_holiday

## Analytics

[analytics](/analytics.md)
