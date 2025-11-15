import json
import pandas as pd
import requests
from io import BytesIO

# Load your JSON data
with open('data.json', 'r') as f:
    data = json.load(f)

print("🔍 Searching for Northern Capacity Data...\n")

# Find the most recent data file
latest_file = None
for item in data.get('@graph', []):
    if item.get('@type') == 'dcat:Distribution':
        title = item.get('dct:title', '')
        url = item.get('dcat:accessURL', {}).get('@id', '') if isinstance(item.get('dcat:accessURL'), dict) else item.get('dcat:accessURL', '')
        
        if 'xlsx' in url.lower() and 'capacity' in title.lower():
            if not latest_file or title > latest_file['title']:
                latest_file = {
                    'title': title,
                    'url': url,
                    'issued': item.get('dct:issued', {}).get('@value', '') if isinstance(item.get('dct:issued'), dict) else item.get('dct:issued', '')
                }

if latest_file:
    print(f"📊 Using latest file: {latest_file['title']}")
    print(f"📅 Date: {latest_file['issued']}")
    print(f"🔗 URL: {latest_file['url']}")
    
    try:
        # Download and analyze the file
        print("\n⏬ Downloading file...")
        response = requests.get(latest_file['url'])
        response.raise_for_status()
        
        # Read Excel file
        excel_file = pd.ExcelFile(BytesIO(response.content))
        
        print(f"📑 Sheets available: {excel_file.sheet_names}")
        
        # Look for sheets that might contain northern data
        northern_sheets = []
        for sheet in excel_file.sheet_names:
            sheet_lower = sheet.lower()
            if any(keyword in sheet_lower for keyword in ['north', 'northern', 'shep', 'scotland', 'total', 'summary']):
                northern_sheets.append(sheet)
        
        # If no obvious northern sheets, use all sheets
        if not northern_sheets:
            northern_sheets = excel_file.sheet_names
        
        print(f"\n🔎 Analyzing sheets: {northern_sheets}")
        
        for sheet_name in northern_sheets:
            print(f"\n📋 Analyzing sheet: {sheet_name}")
            df = pd.read_excel(BytesIO(response.content), sheet_name=sheet_name)
            
            # Look for the specific columns we need
            connected_col = None
            accepted_col = None
            region_col = None
            
            for col in df.columns:
                col_lower = str(col).lower()
                
                if 'connected' in col_lower and 'capacity' in col_lower:
                    connected_col = col
                elif 'accepted' in col_lower and 'capacity' in col_lower:
                    accepted_col = col
                elif any(keyword in col_lower for keyword in ['region', 'area', 'zone', 'north', 'shep']):
                    region_col = col
            
            print(f"📍 Region column: {region_col}")
            print(f"🔌 Connected capacity column: {connected_col}")
            print(f"✅ Accepted capacity column: {accepted_col}")
            
            if region_col and (connected_col or accepted_col):
                # Filter for northern regions
                northern_keywords = ['north', 'northern', 'shep', 'scotland', 'highland']
                northern_data = df[df[region_col].astype(str).str.lower().str.contains('|'.join(northern_keywords), na=False)]
                
                if not northern_data.empty:
                    print(f"\n🏴️ NORTHERN REGIONS FOUND ({len(northern_data)} entries):")
                    
                    if connected_col and pd.api.types.is_numeric_dtype(northern_data[connected_col]):
                        total_connected = northern_data[connected_col].sum()
                        print(f"🔌 TOTAL CONNECTED REGISTERED CAPACITY: {total_connected:,.2f} MW")
                    
                    if accepted_col and pd.api.types.is_numeric_dtype(northern_data[accepted_col]):
                        total_accepted = northern_data[accepted_col].sum()
                        print(f"✅ TOTAL ACCEPTED REGISTERED CAPACITY: {total_accepted:,.2f} MW")
                    
                    # Show the northern regions found
                    print(f"\n📍 Northern regions identified:")
                    for region in northern_data[region_col].unique():
                        print(f"   - {region}")
                
            elif connected_col or accepted_col:
                # If no region column found, show totals for the whole sheet
                print(f"\n📈 OVERALL TOTALS (entire sheet):")
                
                if connected_col and pd.api.types.is_numeric_dtype(df[connected_col]):
                    total_connected = df[connected_col].sum()
                    print(f"🔌 TOTAL CONNECTED REGISTERED CAPACITY: {total_connected:,.2f} MW")
                
                if accepted_col and pd.api.types.is_numeric_dtype(df[accepted_col]):
                    total_accepted = df[accepted_col].sum()
                    print(f"✅ TOTAL ACCEPTED REGISTERED CAPACITY: {total_accepted:,.2f} MW")
            
            else:
                print("❌ No capacity columns found in this sheet")
                
    except Exception as e:
        print(f"❌ Error: {e}")

else:
    print("❌ No data files found!")