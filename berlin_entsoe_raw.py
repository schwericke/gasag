#!/usr/bin/env python3
"""
Berlin ENTSO-E data explorer using raw API calls for specific Berlin parameters.
"""

import os
import pandas as pd
from datetime import datetime, timedelta
import requests
from dotenv import load_dotenv
import pytz

# Load environment variables from .env file
load_dotenv()

def query_entsoe_raw(security_token, document_type, process_type, in_domain, out_domain=None,
                    start_date=None, end_date=None, psr_type=None):
    """
    Raw ENTSO-E API query with specific parameters.
    """
    base_url = "https://transparency.entsoe.eu/api"

    params = {
        'securityToken': security_token,
        'documentType': document_type,
        'processType': process_type,
        'in_Domain': in_domain,
        'periodStart': start_date.strftime('%Y%m%d%H%M'),
        'periodEnd': end_date.strftime('%Y%m%d%H%M')
    }

    if out_domain:
        params['out_Domain'] = out_domain
    if psr_type:
        params['psrType'] = psr_type

    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"API request failed: {e}")
        return None

def main():
    # Check for API key
    api_key = os.getenv('ENTSOE_API_KEY')
    if not api_key:
        print("Please set ENTSOE_API_KEY environment variable")
        return

    # Set time range (last 7 days)
    end_date = datetime.now(pytz.timezone('Europe/Berlin'))
    start_date = end_date - timedelta(days=7)

    print(f"Fetching Berlin energy data using raw ENTSO-E API")
    print(f"Time range: {start_date} to {end_date}")
    print("=" * 60)

    # Create output directory
    os.makedirs('data/entsoe', exist_ok=True)

    # Berlin-specific parameters
    berlin_params = {
        'documentType': 'A73',  # Actual generation per unit
        'processType': 'A16',   # Actual data
        'in_Domain': '10YDE-VE--2',  # 50Hertz control area (includes Berlin)
        'start_date': start_date,
        'end_date': end_date
    }

    print(f"\n🏭 Querying Berlin actual generation per unit...")
    print(f"  Document Type: {berlin_params['documentType']}")
    print(f"  Process Type: {berlin_params['processType']}")
    print(f"  Domain: {berlin_params['in_Domain']} (50Hertz control area)")

    # Query the raw API
    raw_data = query_entsoe_raw(
        security_token=api_key,
        document_type=berlin_params['documentType'],
        process_type=berlin_params['processType'],
        in_domain=berlin_params['in_Domain'],
        start_date=berlin_params['start_date'],
        end_date=berlin_params['end_date']
    )

    if raw_data:
        # Save raw XML response
        with open('data/entsoe/berlin_raw_response.xml', 'w', encoding='utf-8') as f:
            f.write(raw_data)
        print(f"  ✅ Saved raw XML response to berlin_raw_response.xml")

        # Try to parse as XML and extract data
        try:
            import xml.etree.ElementTree as ET
            root = ET.fromstring(raw_data)

            # Look for time series data
            time_series = root.findall('.//{*}TimeSeries')
            print(f"  📊 Found {len(time_series)} time series in response")

            if time_series:
                # Extract data from first time series
                first_series = time_series[0]

                # Get period information
                period = first_series.find('.//{*}Period')
                if period:
                    time_interval = period.find('.//{*}timeInterval')
                    if time_interval:
                        start = time_interval.find('.//{*}start').text
                        end = time_interval.find('.//{*}end').text
                        print(f"  📅 Time interval: {start} to {end}")

                # Get points
                points = period.findall('.//{*}Point') if period else []
                print(f"  📈 Found {len(points)} data points")

                # Extract point data
                data_points = []
                for i, point in enumerate(points[:10]):  # Show first 10 points
                    position = point.find('.//{*}position').text
                    quantity = point.find('.//{*}quantity').text
                    data_points.append({
                        'position': position,
                        'quantity': quantity
                    })
                    print(f"    Point {position}: {quantity}")

                # Save extracted data as CSV
                if data_points:
                    df = pd.DataFrame(data_points)
                    df.to_csv('data/entsoe/berlin_extracted_data.csv', index=False)
                    print(f"  ✅ Saved extracted data to berlin_extracted_data.csv")

        except ET.ParseError as e:
            print(f"  ❌ Failed to parse XML: {e}")
        except Exception as e:
            print(f"  ❌ Error processing XML: {e}")
    else:
        print(f"  ❌ No data received from API")

    # Try alternative approach with different document types
    print(f"\n🔄 Trying alternative document types...")

    alternative_docs = [
        ('A75', 'A16', 'Actual aggregated generation per type'),
        ('A74', 'A16', 'Actual generation output per generation unit'),
        ('A71', 'A16', 'Aggregated generation per type'),
    ]

    for doc_type, proc_type, description in alternative_docs:
        print(f"  📄 {description} (Document Type: {doc_type})...")

        alt_data = query_entsoe_raw(
            security_token=api_key,
            document_type=doc_type,
            process_type=proc_type,
            in_domain=berlin_params['in_Domain'],
            start_date=berlin_params['start_date'],
            end_date=berlin_params['end_date']
        )

        if alt_data and len(alt_data) > 100:  # Check if we got meaningful data
            filename = f'data/entsoe/berlin_{doc_type}_{proc_type}.xml'
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(alt_data)
            print(f"    ✅ Saved to {filename}")
        else:
            print(f"    ⚠️  No data or empty response")

    print(f"\n🎉 Raw API queries complete!")
    print(f"📁 Check the 'data/entsoe/' directory for XML and CSV files.")

if __name__ == "__main__":
    main()
