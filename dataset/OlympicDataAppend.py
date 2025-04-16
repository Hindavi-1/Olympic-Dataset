import pandas as pd
import os
import numpy as np

def append_olympic_data(existing_athletes_path, existing_nocs_path, output_dir='./updated_data'):
    """
    Append Olympic data from downloaded files to existing dataset
    
    Args:
        existing_athletes_path: Path to your existing athlete_events.csv
        existing_nocs_path: Path to your existing noc_regions.csv
        output_dir: Directory to save updated files
    """
    # Create output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Load existing datasets
    print(f"Loading existing datasets from {existing_athletes_path} and {existing_nocs_path}")
    existing_athletes = pd.read_csv(existing_athletes_path)
    existing_nocs = pd.read_csv(existing_nocs_path)
    
    # Get the last ID from existing data
    last_id = existing_athletes['ID'].max()
    next_id = last_id + 1
    print(f"Last athlete ID in existing data: {last_id}")
    
    all_new_data = []
    
    # Process each dataset if it exists
    dataset_files = {
        "PyeongChang 2018": {
            "path": "dataset/winter_2018.csv", 
            "year": 2018, 
            "season": "Winter", 
            "city": "PyeongChang"
        },
        "Tokyo 2020": {
            "path": "dataset/Tokyo 2021.csv", 
            "year": 2020, 
            "season": "Summer", 
            "city": "Tokyo"
        },
        "Beijing 2022": {
            "path": "dataset/Beijing2022.csv", 
            "year": 2022, 
            "season": "Winter", 
            "city": "Beijing"
        },
        "Paris 2024": {
            "path": "dataset/paris 2024.csv", 
            "year": 2024, 
            "season": "Summer", 
            "city": "Paris"
        }
    }
    
    for name, info in dataset_files.items():
        filepath = info["path"]
        year = info["year"]
        season = info["season"]
        city = info["city"]
        
        if not os.path.exists(filepath):
            print(f"Warning: Could not find {name} data file at {filepath}")
            continue
            
        print(f"\nProcessing {name} Olympics data from {filepath}")
        
        try:
            # Load the dataset
            df = pd.read_csv(filepath)
            print(f"Loaded data with {len(df)} rows and columns: {df.columns.tolist()}")
            
            # Helper function to find best matching column
            def find_column(possible_names, df_columns):
                for name in possible_names:
                    matches = [col for col in df_columns if name.lower() in col.lower()]
                    if matches:
                        return matches[0]
                return None
            
            # Map columns - dynamically find the best matching columns
            name_col = find_column(['Name', 'Athlete', 'Participant'], df.columns)
            sex_col = find_column(['Sex', 'Gender'], df.columns)
            team_col = find_column(['Team', 'Country', 'Nation'], df.columns)
            noc_col = find_column(['NOC', 'Country Code', 'CountryCode', 'Code'], df.columns)
            sport_col = find_column(['Sport', 'Discipline'], df.columns)
            event_col = find_column(['Event'], df.columns)
            medal_col = find_column(['Medal'], df.columns)
            
            print("Identified columns:")
            print(f"  Name: {name_col}")
            print(f"  Sex: {sex_col}")
            print(f"  Team: {team_col}")
            print(f"  NOC: {noc_col}")
            print(f"  Sport: {sport_col}")
            print(f"  Event: {event_col}")
            print(f"  Medal: {medal_col}")
            
            if not all([name_col, sex_col, noc_col, sport_col, event_col]):
                print(f"Warning: Could not identify all required columns in {name} dataset")
                continue
            
            # Create mapped data
            mapped_data = []
            
            for _, row in df.iterrows():
                # Extract name
                name = str(row[name_col]) if pd.notna(row[name_col]) else "Unknown"
                
                # Extract and standardize sex
                sex = None
                if sex_col and pd.notna(row[sex_col]):
                    sex_val = str(row[sex_col]).lower().strip()
                    if sex_val in ['m', 'men', 'male', 'man']:
                        sex = 'M'
                    elif sex_val in ['f', 'women', 'female', 'woman']:
                        sex = 'F'
                
                # Extract team
                team = str(row[team_col]) if team_col and pd.notna(row[team_col]) else "Unknown"
                
                # Extract NOC
                noc = str(row[noc_col]) if pd.notna(row[noc_col]) else "Unknown"
                
                # Extract sport
                sport = str(row[sport_col]) if pd.notna(row[sport_col]) else "Unknown"
                
                # Extract event
                event = str(row[event_col]) if pd.notna(row[event_col]) else "Unknown"
                
                # Extract and standardize medal
                medal = "NA"
                if medal_col and pd.notna(row[medal_col]) and str(row[medal_col]).strip():
                    medal_val = str(row[medal_col]).lower().strip()
                    if medal_val in ['gold', 'g']:
                        medal = 'Gold'
                    elif medal_val in ['silver', 's']:
                        medal = 'Silver'
                    elif medal_val in ['bronze', 'b']:
                        medal = 'Bronze'
                
                # Create entry with format matching existing data
                entry = {
                    'Name': name,
                    'Sex': sex,
                    'Age': None,  # None since not available in most of new datasets
                    'Height': None,
                    'Weight': None,
                    'Team': team,
                    'NOC': noc,
                    'Games': f"{year} {season}",
                    'Year': year,
                    'Season': season,
                    'City': city,
                    'Sport': sport,
                    'Event': event,
                    'Medal': medal
                }
                
                mapped_data.append(entry)
            
            # Create DataFrame
            mapped_df = pd.DataFrame(mapped_data)
            print(f"Created {len(mapped_df)} formatted entries for {name} Olympics")
            
            # Add to collection of new data
            all_new_data.append(mapped_df)
            
            # Save individual dataset for reference
            mapped_df.to_csv(f"{output_dir}/{year}_{season.lower()}_processed.csv", index=False)
            
        except Exception as e:
            print(f"Error processing {name} data: {str(e)}")
    
    if not all_new_data:
        print("No data was successfully processed. Please check the file paths and formats.")
        return
    
    # Combine all new data
    new_data = pd.concat(all_new_data, ignore_index=True)
    print(f"\nTotal new entries: {len(new_data)}")
    
    # Add ID column
    new_data['ID'] = range(next_id, next_id + len(new_data))
    
    # Make sure we have all required columns
    for col in existing_athletes.columns:
        if col not in new_data.columns:
            print(f"Adding missing column: {col}")
            new_data[col] = np.nan
    
    # Ensure column order matches existing data
    new_data = new_data[existing_athletes.columns]
    
    # Convert data types to match existing data
    for col in existing_athletes.columns:
        try:
            new_data[col] = new_data[col].astype(existing_athletes[col].dtype)
        except:
            print(f"Could not convert column {col} to {existing_athletes[col].dtype}")
    
    # Save the new data separately for reference
    new_data.to_csv(f"{output_dir}/new_olympic_data.csv", index=False)
    print(f"Saved all new data to {output_dir}/new_olympic_data.csv")
    
    # Append to existing data
    combined_athletes = pd.concat([existing_athletes, new_data], ignore_index=True)
    
    # Save updated athlete data
    combined_athletes.to_csv(f"{output_dir}/athlete_events_updated.csv", index=False)
    print(f"Updated athlete data saved to {output_dir}/athlete_events_updated.csv")
    
    # Update NOC data if needed
    existing_nocs_list = existing_nocs['NOC'].tolist()
    new_nocs = new_data[~new_data['NOC'].isin(existing_nocs_list)]['NOC'].unique()
    
    if len(new_nocs) > 0:
        print(f"Found {len(new_nocs)} new NOC codes to add")
        
        # Known NOC mappings - add any you know
        noc_to_region = {
            'ROC': 'Russian Olympic Committee',
            'EOR': 'Refugee Olympic Team',
            'HKG': 'Hong Kong',
            'PUR': 'Puerto Rico',
            'KOS': 'Kosovo',
            'TPE': 'Chinese Taipei',
            'CHI': 'Chile',
            'PHI': 'Philippines',
            'CGO': 'Republic of the Congo',
            'MKD': 'North Macedonia',
            'UKR': 'Ukraine'
            # Add more mappings as needed
        }
        
        new_nocs_data = []
        for noc in new_nocs:
            region = noc_to_region.get(noc, noc)  # Use mapping if available, otherwise use NOC code
            new_nocs_data.append({
                'NOC': noc,
                'region': region,
                'notes': 'Added from recent Olympics'
            })
        
        new_nocs_df = pd.DataFrame(new_nocs_data)
        combined_nocs = pd.concat([existing_nocs, new_nocs_df], ignore_index=True)
        combined_nocs.to_csv(f"{output_dir}/noc_regions_updated.csv", index=False)
        print(f"Updated NOC data saved to {output_dir}/noc_regions_updated.csv")
    else:
        # Just copy the existing NOC data
        existing_nocs.to_csv(f"{output_dir}/noc_regions_updated.csv", index=False)
        print("No new NOC codes found, copied existing NOC data")

# Call the function with your file paths
if __name__ == "__main__":
    existing_athletes_path = "athlete_events.csv"
    existing_nocs_path = "noc_regions.csv"
    append_olympic_data(existing_athletes_path, existing_nocs_path)