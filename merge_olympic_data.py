import pandas as pd
import os
from datetime import datetime

def calculate_age(birth_date, olympic_year):
    if pd.isna(birth_date):
        return None
    birth_date = datetime.strptime(birth_date, '%Y-%m-%d')
    olympic_date = datetime(olympic_year, 7, 23)  # Olympics start date
    age = olympic_date.year - birth_date.year - ((olympic_date.month, olympic_date.day) < (birth_date.month, birth_date.day))
    return age

def convert_height(height_str):
    if pd.isna(height_str):
        return None
    try:
        # Extract the metric height (first part before '/')
        height_m = float(height_str.split('/')[0])
        return height_m * 100  # Convert to cm
    except:
        return None

def process_2024_data():
    # Read existing merged data
    existing_data = pd.read_csv('athlete_events_merged.csv')
    
    # Read 2024 data
    athletes_2024 = pd.read_csv('Olymics2024/athletes.csv')
    medals_2024 = pd.read_csv('Olymics2024/medals.csv')
    
    # Print column names to help debug
    print("Columns in 2024 athletes data:", athletes_2024.columns.tolist())
    
    # Create a mapping dictionary for medals
    medal_mapping = {
        'Gold Medal': 'Gold',
        'Silver Medal': 'Silver',
        'Bronze Medal': 'Bronze'
    }
    
    # Create a new DataFrame with the same structure as existing data
    new_data = pd.DataFrame()
    
    # Map columns from 2024 data to existing format
    new_data['ID'] = range(len(existing_data) + 1, len(existing_data) + len(athletes_2024) + 1)
    new_data['Name'] = athletes_2024['name']
    new_data['Sex'] = athletes_2024['gender'].map({'Female': 'F', 'Male': 'M', 'W': 'F', 'X': 'M'})
    new_data['Age'] = athletes_2024['birth_date'].apply(lambda x: calculate_age(x, 2024))
    
    # Handle height and weight
    new_data['Height'] = athletes_2024['height'].apply(convert_height)
    new_data['Weight'] = athletes_2024['weight']  # Use the weight column directly
    
    new_data['Team'] = athletes_2024['country']
    new_data['NOC'] = athletes_2024['country_code']
    new_data['Games'] = '2024 Summer'
    new_data['Year'] = 2024
    new_data['Season'] = 'Summer'
    new_data['City'] = 'Paris'
    new_data['Sport'] = athletes_2024['disciplines']  # Changed from 'discipline' to 'disciplines'
    new_data['Event'] = athletes_2024['events']  # Changed from 'discipline' to 'events'
    
    # Initialize Medal column as NA
    new_data['Medal'] = None
    
    # Update medal information
    for _, medal_row in medals_2024.iterrows():
        athlete_name = medal_row['name']
        event = medal_row['event']
        medal = medal_mapping.get(medal_row['medal_type'], None)
        
        # Update the medal for matching athlete and event
        mask = (new_data['Name'] == athlete_name) & (new_data['Event'].str.contains(event.split(' ')[0]))
        new_data.loc[mask, 'Medal'] = medal
        new_data.loc[mask, 'Event'] = event
    
    # Combine with existing data
    final_data = pd.concat([existing_data, new_data], ignore_index=True)
    
    # Save the merged data
    final_data.to_csv('athlete_events_merged.csv', index=False)
    print("2024 Olympics data merged successfully!")

def update_noc_regions():
    # Read existing NOC regions data
    existing_noc_regions = pd.read_csv('noc_regions_merged.csv')
    
    # Read 2024 data to get any new NOC-region mappings
    athletes_2024 = pd.read_csv('Olymics2024/athletes.csv')
    
    # Create new NOC-region pairs from 2024 data
    new_noc_regions = athletes_2024[['country_code', 'country']].drop_duplicates()
    new_noc_regions.columns = ['NOC', 'region']
    new_noc_regions['notes'] = None
    
    # Combine with existing data, keeping only unique NOC entries
    final_noc_regions = pd.concat([existing_noc_regions, new_noc_regions]).drop_duplicates(subset=['NOC'])
    
    # Save the merged data
    final_noc_regions.to_csv('noc_regions_merged.csv', index=False)
    print("NOC regions updated successfully!")

if __name__ == "__main__":
    print("Starting 2024 data merge process...")
    process_2024_data()
    update_noc_regions()
    print("Data merge completed!") 