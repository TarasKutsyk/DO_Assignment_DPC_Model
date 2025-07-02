import pandas as pd

def clean_school_data(url):
    """
    Downloads Chicago Public Schools data, cleans it, and saves it as
    a standardized CSV for use as client points.
    """
    print("Downloading and processing school data (client points)...")
    df = pd.read_csv(url, low_memory=False)

    # --- Data Cleaning Steps ---
    # 1. Select and rename columns
    df_clients = df[[
        'School_ID',
        'Short_Name',
        'Student_Count_Total',
        'School_Latitude',
        'School_Longitude'
    ]].copy()
    df_clients.rename(columns={
        'School_ID': 'id',
        'Short_Name': 'name',
        'Student_Count_Total': 'population',
        'School_Latitude': 'latitude',
        'School_Longitude': 'longitude'
    }, inplace=True)

    # 2. Handle missing population and ensure integer
    df_clients['population'] = df_clients['population'].fillna(0).astype(int)

    # 3. Drop any rows missing coords
    df_clients.dropna(subset=['latitude', 'longitude'], inplace=True)

    # 4. Reorder and save
    df_clients = df_clients[['id', 'name', 'latitude', 'longitude', 'population']]
    output_path = 'chicago_clients.csv'
    df_clients.to_csv(output_path, index=False)
    print(f"Saved {len(df_clients)} client points to '{output_path}'.")
    return output_path

def clean_library_data(url):
    """
    Downloads Chicago Public Libraries data, cleans it, and saves it as
    a standardized CSV for use as candidate facility locations.
    """
    print("Downloading and processing library data (candidate locations)...")
    df = pd.read_csv(url, low_memory=False)

    # --- Data Cleaning Steps ---
    # 1. Select name, address, and the WKT LOCATION field
    df_candidates = df[['BRANCH', 'ADDRESS', 'LOCATION']].copy()

    # 2. Parse 'LOCATION' of the form "(lat, lon)"
    lat_lon = df_candidates['LOCATION'].str.extract(
        r'\(\s*([0-9\.\-]+),\s*([0-9\.\-]+)\s*\)'
    )
    df_candidates['latitude'] = pd.to_numeric(lat_lon[0], errors='coerce')
    df_candidates['longitude'] = pd.to_numeric(lat_lon[1], errors='coerce')

    # 3. Drop the original 'LOCATION' and any rows missing coords
    df_candidates.drop(columns=['LOCATION'], inplace=True)
    df_candidates.dropna(subset=['latitude', 'longitude'], inplace=True)

    # 4. Rename and add unique IDs (starting from 1001)
    df_candidates.rename(columns={'BRANCH': 'name'}, inplace=True)
    df_candidates.reset_index(drop=True, inplace=True)
    df_candidates.insert(0, 'id', df_candidates.index + 1001)

    # 5. Reorder and save
    df_candidates = df_candidates[['id', 'name', 'ADDRESS', 'latitude', 'longitude']]
    output_path = 'chicago_candidates.csv'
    df_candidates.to_csv(output_path, index=False)
    print(f"Saved {len(df_candidates)} candidate locations to '{output_path}'.")
    return output_path

if __name__ == '__main__':
    schools_url = 'https://data.cityofchicago.org/api/views/cu4u-b4d9/rows.csv?accessType=DOWNLOAD'
    libraries_url = 'https://data.cityofchicago.org/api/views/x8fc-8rcq/rows.csv?accessType=DOWNLOAD'

    clean_school_data(schools_url)
    clean_library_data(libraries_url)
    print("Data preparation complete.")
