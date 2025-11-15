import pandas as pd

# URL for the "Latest" version from the provided metadata
file_url = "https://data-api.ssen.co.uk/dataset/655eb750-7b3f-4c55-93b6-9bc4c26a57ab/resource/d421d6cc-6d61-45eb-b70c-0f4c0484e8b2/download/embedded-capacity-register-4.0_november_2025.xlsx"

# Load the Excel file
# Note: 'sheet_name' may need adjustment if the data isn't on the first sheet
df = pd.read_excel(file_url, header=1) # Header often on row 2 (index 1) for these reports

# Filter for Scottish Hydro Electric Power Distribution (SHEPD)
# Using str.contains for robustness in case of whitespace or minor variations
scotland_data = df[df['Licence Area'].str.contains('SHEPD', na=False)]

# Display the first few rows of the Scottish data
print(scotland_data.head())

# Option: Save to a new CSV
# scotland_data.to_csv("scotland_capacity_register.csv", index=False)