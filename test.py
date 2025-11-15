import requests
import pandas as pd
import io

def get_scotland_capacity_data():
    """
    Fetches the SSEN Embedded Capacity Register, finds the latest 50kW CSV,
    downloads it, and filters it for the SHEPD (Scotland) license area.
    """
    
    # 1. --- Query the main API for the dataset's metadata ---
    # This URL points to the "Embedded Capacity Register" dataset.
    metadata_api_url = "https://data-api.ssen.co.uk/api/3/action/package_show?id=embedded_capacity_register"
    
    print(f"Fetching metadata from {metadata_api_url}...")
    
    try:
        response = requests.get(metadata_api_url)
        response.raise_for_status()  # Raise an error for bad status codes
        dataset_info = response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching metadata: {e}")
        return

    if not dataset_info.get("success"):
        print("Error: API did not return a successful response.")
        print(dataset_info)
        return

    # 2. --- Find the latest 50kW CSV file URL from the metadata ---
    # We loop through the "resources" to find the one we want.
    # We'll look for the resource that has "50kW" and "CSV" in its name.
    
    csv_url = None
    resources = dataset_info.get("result", {}).get("resources", [])
    
    for resource in resources:
        name = resource.get("name", "").lower()
        file_format = resource.get("format", "").lower()
        
        if "50kw" in name and "csv" in name and file_format == "csv":
            csv_url = resource.get("url")
            print(f"Found data file: {resource.get('name')}")
            print(f"Downloading from: {csv_url}")
            break # Stop after finding the first match
            
    if not csv_url:
        print("Error: Could not find the 50kW CSV file URL in the API response.")
        return

    # 3. --- Download the CSV data ---
    try:
        print("Downloading data...")
        data_response = requests.get(csv_url)
        data_response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error downloading data file: {e}")
        return

    # 4. --- Process the data with pandas ---
    # We use 'io.StringIO' to treat the downloaded text as a file.
    try:
        print("Processing data with pandas...")
        # Read the CSV data into a pandas DataFrame
        # We specify low_memory=False to help with potential mixed data types.
        data = io.StringIO(data_response.text)
        df = pd.read_csv(data, low_memory=False)

        # 5. --- Filter for Scotland (SHEPD) ---
        # Based on the data definitions, the column is 'DNO Licence Area'.
        # We filter the DataFrame to find rows where this column is 'SHEPD'.
        
        # Check if the column exists
        column_name = 'DNO Licence Area'
        if column_name not in df.columns:
            print(f"Error: Column '{column_name}' not found in the data.")
            print(f"Available columns are: {df.columns.to_list()}")
            return
            
        print(f"Filtering for '{column_name}' == 'SHEPD'...")
        scotland_data = df[df[column_name] == 'SHEPD'].copy()

        # 6. --- Display Results ---
        if scotland_data.empty:
            print("No data found for SHEPD (Scotland).")
        else:
            print(f"\n--- Found {len(scotland_data)} entries for Scotland (SHEPD) ---")
            
            # --- New: Calculate the sum for requested columns ---
            columns_to_sum = [
                'Accepted Registered Capacity', 
                'Connected Registered Capacity'
            ]
            
            for col in columns_to_sum:
                if col not in scotland_data.columns:
                    print(f"Warning: Column '{col}' not found. Skipping.")
                    continue
                
                # Convert column to numeric. 
                # The data might contain commas, so we remove them.
                # errors='coerce' will turn unparseable values into NaN (Not a Number)
                scotland_data[col] = pd.to_numeric(
                    scotland_data[col].astype(str).str.replace(',', ''), 
                    errors='coerce'
                )
                
                # Calculate the sum, ignoring NaN values
                total_sum = scotland_data[col].sum()
                
                print(f"\nTotal for '{col}': {total_sum:,.2f}")

            # Optionally, save the original filtered data to a new CSV
            output_filename = "scotland_embedded_capacity.csv"
            scotland_data.to_csv(output_filename, index=False)
            print(f"\nFull filtered data for Scotland saved to '{output_filename}'")

    except pd.errors.ParserError as e:
        print(f"Error parsing CSV data: {e}")
    except Exception as e:
        print(f"An error occurred during data processing: {e}")


if __name__ == "__main__":
    # To run this script, you need to install 'requests' and 'pandas':
    # pip install requests pandas
    
    get_scotland_capacity_data()