import os
import json

def rename_teams(directory):
    team_mappings = {
        "Royal Challengers Bengaluru": "Royal Challengers Bangalore",
    }

    def update_team_names(obj):
        if isinstance(obj, dict):
            items = list(obj.items())
            for key, value in items:
                if isinstance(value, str) and value in team_mappings:
                    obj[key] = team_mappings[value]
                elif isinstance(value, (dict, list)):
                    update_team_names(value)
                if key in team_mappings:
                    new_key = team_mappings[key]
                    obj[new_key] = obj.pop(key)
        elif isinstance(obj, list):
            for i, item in enumerate(obj):
                if isinstance(item, str) and item in team_mappings:
                    obj[i] = team_mappings[item]
                elif isinstance(item, (dict, list)):
                    update_team_names(item)

    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.json'):
                file_path = os.path.join(root, file)
                
                with open(file_path, 'r', encoding='utf-8') as f:
                    try:
                        data = json.load(f)
                    except json.JSONDecodeError:
                        print(f"Error decoding JSON in file: {file_path}")
                        continue

                original_data = json.dumps(data)
                update_team_names(data)
                modified_data = json.dumps(data)

                if original_data != modified_data:
                    with open(file_path, 'w', encoding='utf-8') as f:
                        json.dump(data, f, indent=2, ensure_ascii=False)
                    print(f"Updated: {file_path}")

# Usage
directory_path = '/Users/goutham/ipl_neo4j/data/ipl_matches/S16-2023'
rename_teams(directory_path)
