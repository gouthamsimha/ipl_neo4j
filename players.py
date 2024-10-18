import os
import json

def get_players_from_file(file_path):
    with open(file_path, 'r') as f:
        data = json.load(f)
    
    players = set()
    if 'info' in data and 'players' in data['info']:
        for team in data['info']['players']:
            players.update(data['info']['players'][team])
    
    return players

def get_all_players(root_dir):
    all_players = set()
    
    for season_folder in sorted(os.listdir(root_dir)):
        season_path = os.path.join(root_dir, season_folder)
        if os.path.isdir(season_path):
            print(f"Processing {season_folder}...")
            for file in os.listdir(season_path):
                if file.endswith('.json'):
                    file_path = os.path.join(season_path, file)
                    players = get_players_from_file(file_path)
                    all_players.update(players)
    
    return sorted(list(all_players))

# Assuming the data is in a directory named 'data/ipl_matches'
root_directory = 'daa/'
unique_players = get_all_players(root_directory)

print(f"Total unique players: {len(unique_players)}")
print("Unique players list:")
for player in unique_players:
    print(player)