#!/usr/bin/env python3
"""
Berlin ENTSO-E data explorer to download energy data for Berlin/Germany.
"""

import os
import pandas as pd
from datetime import datetime, timedelta
from entsoe import EntsoePandasClient
from dotenv import load_dotenv
import pytz

# Load environment variables from .env file
load_dotenv()

def main():
    # Check for API key
    api_key = os.getenv('ENTSOE_API_KEY')
    if not api_key:
        print("Please set ENTSOE_API_KEY environment variable")
        return

    # Initialize client
    client = EntsoePandasClient(api_key=api_key)

    # Set time range (last 7 days for more data)
    end_date = pd.Timestamp.now(tz='Europe/Berlin')
    start_date = end_date - timedelta(days=7)

    print(f"Fetching Berlin/Germany energy data from {start_date} to {end_date}")
    print("=" * 60)

    # Germany area codes (Berlin is part of Germany)
    # 10YDE-VE--2 is the 50Hertz control area which includes Berlin
    germany_areas = ['DE_LU', 'DE_AT_LU', 'DE', '10YDE-VE--2']

    # Create output directory
    os.makedirs('data/entsoe', exist_ok=True)

    for area in germany_areas:
        print(f"\n📊 Fetching data for area: {area}")

        # 1. Day ahead prices
        try:
            print(f"  🔌 Day ahead prices...")
            prices = client.query_day_ahead_prices(area, start=start_date, end=end_date)
            if not prices.empty:
                prices_df = prices.reset_index()
                prices_df.columns = ['timestamp', 'price_eur_mwh']
                prices_df['area'] = area
                prices_df.to_csv(f'data/entsoe/day_ahead_prices_{area}.csv', index=False)
                print(f"    ✅ Saved {len(prices_df)} price records")
                print(f"    📈 Price range: {prices_df['price_eur_mwh'].min():.2f} - {prices_df['price_eur_mwh'].max():.2f} €/MWh")
            else:
                print(f"    ⚠️  No price data available")
        except Exception as e:
            print(f"    ❌ Error fetching prices: {e}")

        # 2. Generation data by type
        generation_types = {
            'B01': 'Biomass',
            'B04': 'Fossil Gas',
            'B16': 'Solar',
            'B19': 'Wind Onshore',
            'B20': 'Wind Offshore',
            'B14': 'Nuclear',
            'B05': 'Fossil Hard coal',
            'B06': 'Fossil Oil',
            'B07': 'Fossil Peat',
            'B08': 'Geothermal',
            'B09': 'Hydro Pumped Storage',
            'B10': 'Hydro Run-of-river and poundage',
            'B11': 'Hydro Water Reservoir',
            'B12': 'Marine',
            'B13': 'Nuclear',
            'B15': 'Other renewable',
            'B17': 'Waste',
            'B18': 'Wind Offshore'
        }

        print(f"  ⚡ Generation data by type...")
        for psr_type, description in generation_types.items():
            try:
                gen_data = client.query_generation(area, start=start_date, end=end_date, psr_type=psr_type)
                if not gen_data.empty:
                    gen_df = gen_data.reset_index()
                    gen_df.columns = ['timestamp', 'generation_mw']
                    gen_df['area'] = area
                    gen_df['generation_type'] = description
                    gen_df['psr_type'] = psr_type
                    gen_df.to_csv(f'data/entsoe/generation_{area}_{psr_type}_{description.replace(" ", "_").lower()}.csv', index=False)
                    print(f"    ✅ {description}: {len(gen_df)} records, avg: {gen_df['generation_mw'].mean():.1f} MW")
            except Exception as e:
                # Skip if no data for this generation type
                pass

        # 3. Installed capacity
        try:
            print(f"  🏭 Installed capacity...")
            capacity = client.query_installed_generation_capacity(area, start=start_date, end=end_date)
            if not capacity.empty:
                capacity_df = capacity.reset_index()
                capacity_df['area'] = area
                capacity_df.to_csv(f'data/entsoe/installed_capacity_{area}.csv', index=False)
                print(f"    ✅ Saved {len(capacity_df)} capacity records")
        except Exception as e:
            print(f"    ❌ Error fetching capacity: {e}")

        # 4. Load data (consumption)
        try:
            print(f"  💡 Load/consumption data...")
            load = client.query_load(area, start=start_date, end=end_date)
            if not load.empty:
                load_df = load.reset_index()
                load_df.columns = ['timestamp', 'load_mw']
                load_df['area'] = area
                load_df.to_csv(f'data/entsoe/load_{area}.csv', index=False)
                print(f"    ✅ Saved {len(load_df)} load records")
                print(f"    📊 Load range: {load_df['load_mw'].min():.1f} - {load_df['load_mw'].max():.1f} MW")
            else:
                print(f"    ⚠️  No load data available")
        except Exception as e:
            print(f"    ❌ Error fetching load: {e}")

    # 5. Cross-border flows (Germany's connections)
    print(f"\n🌍 Cross-border flows...")
    try:
        flows = client.query_crossborder_flows('DE_LU', start=start_date, end=end_date)
        if not flows.empty:
            flows_df = flows.reset_index()
            flows_df['area'] = 'DE_LU'
            flows_df.to_csv(f'data/entsoe/crossborder_flows_DE_LU.csv', index=False)
            print(f"  ✅ Saved {len(flows_df)} flow records")
        else:
            print(f"  ⚠️  No cross-border flow data available")
    except Exception as e:
        print(f"  ❌ Error fetching cross-border flows: {e}")

    # 6. Berlin-specific actual generation per unit (50Hertz control area)
    print(f"\n🏭 Berlin-specific actual generation per unit (50Hertz control area)...")
    try:
        # Using the specific parameters for Berlin area
        berlin_gen = client.query_generation_per_plant(
            country_code='10YDE-VE--2',  # 50Hertz control area (includes Berlin)
            start=start_date,
            end=end_date,
            psr_type=None  # All generation types
        )
        if not berlin_gen.empty:
            berlin_gen_df = berlin_gen.reset_index()
            berlin_gen_df['area'] = '10YDE-VE--2'
            berlin_gen_df['document_type'] = 'A73'  # Actual generation per unit
            berlin_gen_df['process_type'] = 'A16'   # Actual data
            berlin_gen_df.to_csv(f'data/entsoe/berlin_actual_generation_per_unit.csv', index=False)
            print(f"  ✅ Saved {len(berlin_gen_df)} Berlin generation records")
            print(f"  📊 Columns: {list(berlin_gen_df.columns)}")
            if len(berlin_gen_df) > 0:
                print(f"  📈 Sample data:\n{berlin_gen_df.head()}")
        else:
            print(f"  ⚠️  No Berlin generation per unit data available")
    except Exception as e:
        print(f"  ❌ Error fetching Berlin generation per unit: {e}")

    # 7. Try alternative method for Berlin generation data
    print(f"\n⚡ Alternative Berlin generation data...")
    try:
        # Try to get generation data specifically for the 50Hertz area
        berlin_gen_alt = client.query_generation(
            country_code='10YDE-VE--2',
            start=start_date,
            end=end_date
        )
        if not berlin_gen_alt.empty:
            berlin_gen_alt_df = berlin_gen_alt.reset_index()
            berlin_gen_alt_df['area'] = '10YDE-VE--2'
            berlin_gen_alt_df['document_type'] = 'A73'
            berlin_gen_alt_df['process_type'] = 'A16'
            berlin_gen_alt_df.to_csv(f'data/entsoe/berlin_generation_alternative.csv', index=False)
            print(f"  ✅ Saved {len(berlin_gen_alt_df)} Berlin generation records (alternative)")
            print(f"  📊 Columns: {list(berlin_gen_alt_df.columns)}")
        else:
            print(f"  ⚠️  No alternative Berlin generation data available")
    except Exception as e:
        print(f"  ❌ Error fetching alternative Berlin generation: {e}")

    print(f"\n🎉 Data download complete! Check the 'data/entsoe/' directory for CSV files.")
    print(f"📁 Files saved:")
    for file in os.listdir('data/entsoe'):
        if file.endswith('.csv'):
            file_path = os.path.join('data/entsoe', file)
            size = os.path.getsize(file_path)
            print(f"   📄 {file} ({size} bytes)")

if __name__ == "__main__":
    main()
