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
        Fetch data for Scotland
        """
        print("Fetching Scotland data from SSE API...")
        
        # Try multiple approaches to get data
        all_records = []
        
        # Approach 1: Get general data
        print("Getting general data sample...")
        general_params = {
            'resource_id': self.resource_id,
            'limit': min(limit, 1000)
        }
        
        try:
            response = requests.get(self.base_url, params=general_params)
            data = response.json()
            
            if data.get('success') and data['result']['records']:
                records = data['result']['records']
                all_records.extend(records)
                print(f"Retrieved {len(records)} general records")
        except Exception as e:
            print(f"Error fetching general data: {e}")
        
        # Approach 2: Try Scottish search terms
        scottish_terms = ["Scotland", "Glasgow", "Edinburgh", "Aberdeen"]
        
        for term in scottish_terms:
            try:
                params = {
                    'resource_id': self.resource_id,
                    'q': term,
                    'limit': 500
                }
                
                response = requests.get(self.base_url, params=params)
                data = response.json()
                
                if data.get('success') and data['result']['records']:
                    records = data['result']['records']
                    # Add only new records
                    for record in records:
                        if record not in all_records:
                            all_records.append(record)
                    print(f"Found {len(records)} records for '{term}'")
                    
            except Exception as e:
                print(f"Error searching for {term}: {e}")
        
        if all_records:
            df = pd.DataFrame(all_records)
            print(f"Total records retrieved: {len(df)}")
            return df
        else:
            print("No records found from API")
            return pd.DataFrame()
    
    def filter_scotland_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Filter DataFrame for Scottish locations
        """
        if df.empty:
            return df
            
        print("Filtering for Scotland data...")
        
        scottish_indicators = [
            'Scotland', 'SCOTLAND', 'scotland',
            'Glasgow', 'Edinburgh', 'Aberdeen', 'Dundee', 'Inverness'
        ]
        
        scottish_postcodes = ['G', 'EH', 'AB', 'DD', 'IV', 'KY', 'FK']
        
        scotland_mask = pd.Series([False] * len(df))
        
        # Check location fields
        location_fields = ['Country', 'County', 'town__city', 'Postcode']
        
        for field in location_fields:
            if field in df.columns:
                field_mask = df[field].astype(str).str.contains(
                    '|'.join(scottish_indicators), 
                    case=False, 
                    na=False
                )
                scotland_mask = scotland_mask | field_mask
        
        # Check postcodes
        if 'Postcode' in df.columns:
            postcode_mask = df['Postcode'].astype(str).str.extract(r'^([A-Z]+)', expand=False)
            postcode_mask = postcode_mask.isin(scottish_postcodes)
            scotland_mask = scotland_mask | postcode_mask
        
        scotland_df = df[scotland_mask].copy()
        print(f"Scotland records after filtering: {len(scotland_df)}")
        
        return scotland_df
    
    def calculate_capacity_totals(self, df: pd.DataFrame) -> Dict:
        """
        Calculate total capacity figures
        """
        print("Calculating capacity totals...")
        
        capacity_fields = {
            'accepted_registered_capacity': 'accepted_to_connect_registered_capacity__mw_',
            'connected_registered_capacity': 'already_connected_registered_capacity__mw_',
            'maximum_export_capacity': 'maximum_export_capacity__mw_',
            'maximum_import_capacity': 'maximum_import_capacity__mw_'
        }
        
        totals = {}
        
        for capacity_name, field_id in capacity_fields.items():
            if field_id in df.columns:
                capacity_series = pd.to_numeric(df[field_id], errors='coerce')
                valid_capacities = capacity_series.dropna()
                total_capacity = valid_capacities.sum()
                
                totals[capacity_name] = {
                    'total_mw': total_capacity,
                    'count_records': len(valid_capacities),
                    'average_mw': total_capacity / len(valid_capacities) if len(valid_capacities) > 0 else 0,
                    'min_mw': valid_capacities.min() if len(valid_capacities) > 0 else 0,
                    'max_mw': valid_capacities.max() if len(valid_capacities) > 0 else 0
                }
            else:
                totals[capacity_name] = {'total_mw': 0, 'count_records': 0, 'average_mw': 0}
        
        return totals