import requests
import pandas as pd
import json
from typing import Dict, List, Optional

class SSEScotlandCapacityAnalyzer:
    """
    A class to extract and sum capacity figures from SSE Scotland data
    """
    
    def __init__(self):
        self.base_url = 'https://ckan-prod.sse.datopian.com/api/3/action/datastore_search'
        self.resource_id = 'd258bd7b-22db-4d32-9450-b3783591b66d'
        
    def fetch_scotland_data(self, limit: int = 32000) -> pd.DataFrame:
        """
        Fetch data for Scotland by searching for Scottish locations and postcodes
        """
        print("Fetching Scotland data from SSE API...")
        
        # Scottish-related search terms
        scottish_terms = [
            "Scotland",
            "Glasgow",
            "Edinburgh",
            "Aberdeen",
            "Dundee",
            "Inverness",
            "Highland",
            "Fife",
            "Lothian",
            "Strathclyde",
            "G1", "G2", "G3", "G4", "G5",  # Glasgow postcodes
            "EH1", "EH2", "EH3", "EH4", "EH5",  # Edinburgh postcodes
            "AB1", "AB2", "AB3", "AB4", "AB5",  # Aberdeen postcodes
            "DD1", "DD2", "DD3", "DD4", "DD5",  # Dundee postcodes
            "IV1", "IV2", "IV3", "IV4", "IV5",  # Inverness postcodes
        ]
        
        all_records = []
        
        for term in scottish_terms:
            print(f"Searching for: {term}")
            params = {
                'resource_id': self.resource_id,
                'q': term,
                'limit': 1000
            }
            
            try:
                response = requests.get(self.base_url, params=params)
                data = response.json()
                
                if data.get('success') and data['result']['records']:
                    records = data['result']['records']
                    all_records.extend(records)
                    print(f"  Found {len(records)} records for '{term}'")
                    
                    # Also search in specific Scottish fields
                    for field in ['Country', 'County', 'town__city']:
                        field_params = {
                            'resource_id': self.resource_id,
                            'filters': json.dumps({field: term}),
                            'limit': 1000
                        }
                        
                        field_response = requests.get(self.base_url, params=field_params)
                        field_data = field_response.json()
                        
                        if field_data.get('success') and field_data['result']['records']:
                            field_records = field_data['result']['records']
                            # Avoid duplicates by checking if record already exists
                            for record in field_records:
                                if record not in all_records:
                                    all_records.append(record)
                            print(f"    Found {len(field_records)} additional records in {field}")
                            
            except Exception as e:
                print(f"Error searching for {term}: {e}")
        
        # Also get a sample without filters to ensure we get data
        print("Getting general sample...")
        general_params = {
            'resource_id': self.resource_id,
            'limit': min(limit, 1000)
        }
        
        try:
            general_response = requests.get(self.base_url, params=general_params)
            general_data = general_response.json()
            
            if general_data.get('success') and general_data['result']['records']:
                general_records = general_data['result']['records']
                for record in general_records:
                    if record not in all_records:
                        all_records.append(record)
                print(f"Added {len(general_records)} general records")
        except Exception as e:
            print(f"Error fetching general data: {e}")
        
        if all_records:
            df = pd.DataFrame(all_records)
            print(f"\nTotal Scotland-related records found: {len(df)}")
            return df
        else:
            print("No records found. Trying different approach...")
            return self.fetch_all_data(limit)
    
    def fetch_all_data(self, limit: int = 10000) -> pd.DataFrame:
        """
        Fetch all data and filter for Scotland later
        """
        print("Fetching all available data...")
        
        all_records = []
        offset = 0
        batch_size = 1000
        
        while len(all_records) < limit:
            params = {
                'resource_id': self.resource_id,
                'limit': batch_size,
                'offset': offset
            }
            
            try:
                response = requests.get(self.base_url, params=params)
                data = response.json()
                
                if not data.get('success') or not data['result']['records']:
                    break
                    
                records = data['result']['records']
                all_records.extend(records)
                offset += batch_size
                
                print(f"Fetched {len(records)} records (total: {len(all_records)})")
                
                if len(records) < batch_size:
                    break
                    
            except Exception as e:
                print(f"Error fetching data at offset {offset}: {e}")
                break
        
        df = pd.DataFrame(all_records)
        print(f"\nTotal records fetched: {len(df)}")
        return df
    
    def filter_scotland_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Filter DataFrame for Scottish locations
        """
        print("\nFiltering for Scotland data...")
        
        # Scottish indicators
        scottish_indicators = [
            'Scotland', 'SCOTLAND', 'scotland',
            'Glasgow', 'Edinburgh', 'Aberdeen', 'Dundee', 'Inverness',
            'Highland', 'Fife', 'Lothian', 'Strathclyde',
            'Argyll', 'Ayrshire', 'Dumfries', 'Galloway', 'Borders'
        ]
        
        # Scottish postcode areas
        scottish_postcodes = ['G', 'EH', 'AB', 'DD', 'IV', 'KY', 'FK', 'KW', 'PA', 'PH', 'TD', 'DG', 'ML', 'KA']
        
        scotland_mask = pd.Series([False] * len(df))
        
        # Check various fields for Scottish indicators
        location_fields = ['Country', 'County', 'town__city', 'Address Line 1', 'Address Line 2', 'Postcode']
        
        for field in location_fields:
            if field in df.columns:
                field_mask = df[field].astype(str).str.contains(
                    '|'.join(scottish_indicators), 
                    case=False, 
                    na=False
                )
                scotland_mask = scotland_mask | field_mask
                print(f"  Found {field_mask.sum()} records in {field}")
        
        # Check for Scottish postcodes
        if 'Postcode' in df.columns:
            postcode_mask = df['Postcode'].astype(str).str.extract(r'^([A-Z]+)', expand=False)
            postcode_mask = postcode_mask.isin(scottish_postcodes)
            scotland_mask = scotland_mask | postcode_mask
            print(f"  Found {postcode_mask.sum()} records with Scottish postcodes")
        
        scotland_df = df[scotland_mask].copy()
        print(f"\nTotal Scotland records after filtering: {len(scotland_df)}")
        
        return scotland_df
    
    def calculate_capacity_totals(self, df: pd.DataFrame) -> Dict:
        """
        Calculate total capacity figures for Scotland
        """
        print("\n=== CALCULATING SCOTLAND CAPACITY TOTALS ===")
        
        capacity_fields = {
            'accepted_registered_capacity': 'accepted_to_connect_registered_capacity__mw_',
            'connected_registered_capacity': 'already_connected_registered_capacity__mw_',
            'maximum_export_capacity': 'maximum_export_capacity__mw_',
            'maximum_import_capacity': 'maximum_import_capacity__mw_',
            'energy_tech_1_capacity': 'energy_source___energy_conversion_technology_1___registered_',
            'energy_tech_2_capacity': 'energy_source___energy_conversion_technology_2___registered_',
            'energy_tech_3_capacity': 'energy_source___energy_conversion_technology_3___registered_'
        }
        
        totals = {}
        
        for capacity_name, field_id in capacity_fields.items():
            if field_id in df.columns:
                # Convert to numeric, handling non-numeric values
                capacity_series = pd.to_numeric(df[field_id], errors='coerce')
                
                # Remove NaN values
                valid_capacities = capacity_series.dropna()
                
                total_capacity = valid_capacities.sum()
                count_records = len(valid_capacities)
                
                totals[capacity_name] = {
                    'total_mw': total_capacity,
                    'count_records': count_records,
                    'average_mw': total_capacity / count_records if count_records > 0 else 0,
                    'min_mw': valid_capacities.min() if count_records > 0 else 0,
                    'max_mw': valid_capacities.max() if count_records > 0 else 0
                }
                
                print(f"\n{capacity_name.replace('_', ' ').title()}:")
                print(f"  Total: {total_capacity:,.2f} MW")
                print(f"  Records with data: {count_records}")
                print(f"  Average: {total_capacity / count_records:,.2f} MW" if count_records > 0 else "  Average: N/A")
                print(f"  Range: {valid_capacities.min():.2f} - {valid_capacities.max():.2f} MW" if count_records > 0 else "  Range: N/A")
            else:
                print(f"\n{capacity_name.replace('_', ' ').title()}: Field not found")
                totals[capacity_name] = {'total_mw': 0, 'count_records': 0, 'average_mw': 0}
        
        # Calculate grand total of all capacities
        grand_total = sum(totals[cap]['total_mw'] for cap in totals)
        print(f"\n{'='*50}")
        print(f"GRAND TOTAL CAPACITY: {grand_total:,.2f} MW")
        print(f"{'='*50}")
        
        return totals
    
    def analyze_connection_status(self, df: pd.DataFrame):
        """
        Analyze connection status distribution
        """
        if 'Connection Status' in df.columns:
            print(f"\n=== CONNECTION STATUS DISTRIBUTION ===")
            status_counts = df['Connection Status'].value_counts()
            for status, count in status_counts.items():
                percentage = (count / len(df)) * 100
                print(f"  {status}: {count} records ({percentage:.1f}%)")
    
    def export_scotland_data(self, df: pd.DataFrame, filename: str = "scotland_capacity_data.csv"):
        """
        Export Scotland data to CSV
        """
        if not df.empty:
            # Select relevant columns
            relevant_columns = [
                'Customer Name', 'Customer Site', 'town__city', 'County', 'Postcode', 'Country',
                'Connection Status', 'accepted_to_connect_registered_capacity__mw_',
                'already_connected_registered_capacity__mw_', 'maximum_export_capacity__mw_',
                'maximum_import_capacity__mw_', 'Date Connected', 'Date Accepted'
            ]
            
            # Filter to available columns
            available_columns = [col for col in relevant_columns if col in df.columns]
            export_df = df[available_columns]
            
            export_df.to_csv(filename, index=False)
            print(f"\nScotland data exported to {filename}")
            print(f"Exported {len(export_df)} records with {len(available_columns)} columns")
        else:
            print("No data to export")

def main():
    """
    Main function to analyze Scotland capacity data
    """
    analyzer = SSEScotlandCapacityAnalyzer()
    
    print("SSE Scotland Capacity Analysis")
    print("=" * 60)
    
    # Fetch data
    df = analyzer.fetch_scotland_data(limit=5000)
    
    if df.empty:
        print("No data retrieved from API. The API might be down or the resource ID might have changed.")
        return
    
    # Filter for Scotland
    scotland_df = analyzer.filter_scotland_data(df)
    
    if scotland_df.empty:
        print("No Scotland-specific data found. Analyzing all available data instead.")
        scotland_df = df
    
    # Calculate capacity totals
    totals = analyzer.calculate_capacity_totals(scotland_df)
    
    # Analyze connection status
    analyzer.analyze_connection_status(scotland_df)
    
    # Display sample of Scottish data
    print(f"\n=== SAMPLE OF SCOTLAND DATA ===")
    if not scotland_df.empty:
        sample_cols = ['Customer Name', 'town__city', 'Postcode', 
                      'accepted_to_connect_registered_capacity__mw_',
                      'already_connected_registered_capacity__mw_']
        available_sample_cols = [col for col in sample_cols if col in scotland_df.columns]
        
        if available_sample_cols:
            sample_df = scotland_df[available_sample_cols].head(10)
            pd.set_option('display.width', None)
            print(sample_df.to_string(index=False))
    
    # Export data
    analyzer.export_scotland_data(scotland_df)
    
    # Summary
    print(f"\n=== SUMMARY ===")
    print(f"Total records analyzed: {len(scotland_df)}")
    print(f"Key capacity figures for Scotland:")
    
    key_capacities = {
        'Accepted Registered Capacity': totals.get('accepted_registered_capacity', {}).get('total_mw', 0),
        'Connected Registered Capacity': totals.get('connected_registered_capacity', {}).get('total_mw', 0),
        'Maximum Export Capacity': totals.get('maximum_export_capacity', {}).get('total_mw', 0)
    }
    
    for name, value in key_capacities.items():
        print(f"  {name}: {value:,.2f} MW")

if __name__ == "__main__":
    main()