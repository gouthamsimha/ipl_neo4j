import os
import json

def update_season(folder_path, new_season):
    for filename in os.listdir(folder_path):
        if filename.endswith('.json'):
            file_path = os.path.join(folder_path, filename)
            
            with open(file_path, 'r') as file:
                data = json.load(file)
            
            if 'info' in data and 'season' in data['info']:
                data['info']['season'] = new_season
            
            with open(file_path, 'w') as file:
                json.dump(data, file, indent=2)
            
            print(f"Updated season in {filename}")

# Usage
folder_path = 'data/ipl_matches/S17-2024'
new_season = 2024
update_season(folder_path, new_season)
