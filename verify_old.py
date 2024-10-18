import json
import glob
import os
import logging
from py2neo import Graph, Node, Relationship, Subgraph
from py2neo.matching import NodeMatcher
from tqdm import tqdm  # For displaying progress bars
from collections import defaultdict
from decimal import Decimal
from concurrent.futures import ThreadPoolExecutor, as_completed

# ------------------------ Configuration ------------------------

# Neo4j connection details
NEO4J_URI = "bolt://localhost:7687"  # Default Neo4j URI
NEO4J_USER = "neo4j"                 # Neo4j Username
NEO4J_PASSWORD = "Myapple7@"         # Replace with your Neo4j Password

# Path to the directories containing JSON files
JSON_DIRS = [
    "/Users/goutham/ipl_neo4j/data/ipl_matches/S17-2024",
    "/Users/goutham/ipl_neo4j/data/ipl_matches/S16-2023",
    "/Users/goutham/ipl_neo4j/data/ipl_matches/S15-2022",
    "/Users/goutham/ipl_neo4j/data/ipl_matches/S14-2021",
    "/Users/goutham/ipl_neo4j/data/ipl_matches/S13-2020",
    "/Users/goutham/ipl_neo4j/data/ipl_matches/S12-2019",
    "/Users/goutham/ipl_neo4j/data/ipl_matches/S11-2018",
    "/Users/goutham/ipl_neo4j/data/ipl_matches/S10-2017",
    "/Users/goutham/ipl_neo4j/data/ipl_matches/S9-2016",
    "/Users/goutham/ipl_neo4j/data/ipl_matches/S8-2015",
    "/Users/goutham/ipl_neo4j/data/ipl_matches/S7-2014",
    "/Users/goutham/ipl_neo4j/data/ipl_matches/S6-2013",
    "/Users/goutham/ipl_neo4j/data/ipl_matches/S5-2012",
    "/Users/goutham/ipl_neo4j/data/ipl_matches/S4-2011",
    "/Users/goutham/ipl_neo4j/data/ipl_matches/S3-2010",
    "/Users/goutham/ipl_neo4j/data/ipl_matches/S2-2009",
    "/Users/goutham/ipl_neo4j/data/ipl_matches/S1-2008",   
    # Add more paths as needed
]

# League name (e.g., "IPL")
LEAGUE_NAME = "Indian Premier League"  # Modify as needed

# Logging configuration
logging.basicConfig(
    filename='importingg.log',
    filemode='w',  # Overwrite the log file each time
    format='%(asctime)s %(levelname)s:%(message)s',
    level=logging.INFO
)

# ------------------------ Connect to Neo4j ------------------------

try:
    # Initialize connection to Neo4j
    graph = Graph(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    matcher = NodeMatcher(graph)
    logging.info("Successfully connected to Neo4j.")
except Exception as e:
    logging.error(f"Failed to connect to Neo4j: {e}")
    raise e

# ------------------------ Helper Functions ------------------------

def get_or_create_league(name, properties=None):
    league = Node("League", name=name)
    if properties:
        league.update(properties)
    graph.merge(league, "League", "name")
    logging.info(f"Created/Merged League: {name} with properties: {properties}")
    return league

def get_or_create_season(year, league_node, properties=None):
    season = Node("Season", year=year)
    if properties:
        season.update(properties)
    graph.merge(season, "Season", "year")
    # Link League to Season (changed direction)
    rel = Relationship(league_node, "HAS_SEASON", season)
    graph.merge(rel)
    logging.info(f"Created/Merged Season: {year} and linked to League: {league_node['name']}")
    return season

# Load team data
with open('teams_data.json', 'r') as f:
    teams_data = json.load(f)

def get_or_create_team(name, season_node):
    team = Node("Team", name=name)
    graph.merge(team, "Team", "name")

    # Update team properties from teams_data.json
    if name in teams_data:
        team_info = teams_data[name]
        for key, value in team_info.items():
            if key not in team or team[key] != value:
                team[key] = value
                graph.push(team)
                logging.info(f"Updated {key} for Team: {name} to {value}")
    else:
        logging.warning(f"Team {name} not found in teams_data.json")

    # Link Team to Season
    rel = Relationship(team, "PARTICIPATED_IN", season_node)
    graph.merge(rel)
    logging.info(f"Created/Merged Team: {name} and linked to Season: {season_node['year']}")
    return team

def get_or_create_player(name, registry_id):
    player = Node(
        "Player",
        registry_id=registry_id,
        name=name,
        full_name="",
        batting_position="",
        role="",
        bowling_style="",
        nationality="",
        date_of_birth="",
        teams_played_for=[],
    )
    graph.merge(player, "Player", "registry_id")
    # Update name if not already set
    if "name" not in player or not player["name"]:
        player["name"] = name
        graph.push(player)
        logging.info(f"Set name for Player: {registry_id} to {name}")
    return player

def get_or_create_official(name, role):
    official = Node("Official", name=name)
    graph.merge(official, "Official", "name")
    # Update role if not already set
    if "role" not in official or not official["role"]:
        official["role"] = role
        graph.push(official)
        logging.info(f"Set role for Official: {name} to {role}")
    return official

def get_or_create_venue(name, city):
    venue = Node("Venue", name=name)
    graph.merge(venue, "Venue", "name")
    # Update city if not already set
    if "city" not in venue or not venue["city"]:
        venue["city"] = city
        graph.push(venue)
        logging.info(f"Set city for Venue: {name} to {city}")
    return venue

def get_phase(over, ball):
    over_ball = Decimal(f"{over}.{ball}")
    if over_ball <= Decimal('5.6'):
        return "Powerplay"
    elif over_ball <= Decimal('15.6'):
        return "Middle Overs"
    else:
        return "Death Overs"

# ------------------------ Import Function ------------------------

def import_json_to_neo4j(json_directory, league_name):
    json_files = glob.glob(os.path.join(json_directory, "*.json"))
    logging.info(f"Found {len(json_files)} JSON files to import.")

    if not json_files:
        logging.warning("No JSON files found to import.")
        return

    # Extract league properties from the first file (assuming all files have the same league info)
    first_file = json_files[0]
    with open(first_file, 'r') as f:
        data = json.load(f)
        info = data.get('info', {})
        event = info.get('event', {})

    league_properties = {
        "name": league_name,
        "country": "India",  # Assuming IPL is always in India
        "format": "T20",  # Assuming IPL is always T20
        "gender": info.get('gender', 'male'),  # Usually 'male' for IPL
        "tournament": event.get('name', league_name),
        "match_type": info.get('match_type', 'T20'),
        "overs": info.get('overs', 20),  # Usually 20 for T20
        "balls_per_over": info.get('balls_per_over', 6),
        "governing_body": "BCCI",
        "founded": 2007,
        "inaugural_season": 2008,
        "logo": "https://www.iplt20.com/assets/images/ipl-logo.png",  # Replace with actual logo path/URL
        "website": "https://www.iplt20.com"
    }

    # Create or retrieve League with properties
    league_node = get_or_create_league(league_name, properties=league_properties)

    # Initialize season statistics
    season_stats = defaultdict(lambda: {
        "total_runs": 0,
        "total_wickets": 0,
        "total_matches": 0,
        "highest_team_score": 0,
        "lowest_team_score": None,
        "total_sixes": 0,
        "total_fours": 0,
        "teams": set(),
        "super_over_matches": 0,  # New counter for super over matches
    })

    # Define a function to process a single file
    def process_file(file):
        try:
            with open(file, 'r') as f:
                data = json.load(f)
                logging.info(f"Loaded JSON file: {file}")
        except json.JSONDecodeError as e:
            logging.error(f"JSON decode error in file {file}: {e}")
            return

        # Extract meta and info
        meta = data.get('meta', {})
        info = data.get('info', {})
        innings_list = data.get('innings', [])
        logging.info(f"innings_list structure: {innings_list}")

        # Generate a unique match_id (e.g., match_number_date)
        match_number = info.get('event', {}).get('match_number')
        match_date = info.get('dates', [None])[0]
        if not match_number or not match_date:
            logging.error(f"Missing match_number or date in file {file}. Skipping.")
            return
        match_id = f"{match_number}_{match_date}"

        # Extract season
        season_year = info.get('season')
        if not season_year:
            logging.error(f"Missing season in file {file}. Skipping.")
            return

        # Create or retrieve Season node and link to League
        season_node = get_or_create_season(season_year, league_node)

        # Extract Teams and create Team nodes linked to Season
        teams = info.get('teams', [])
        if len(teams) != 2:
            logging.error(f"Invalid number of teams in file {file}. Skipping.")
            return
        team1_name, team2_name = teams

        # Add teams to the season_stats
        season_stats[season_year]["teams"].add(team1_name)
        season_stats[season_year]["teams"].add(team2_name)

        team1_node = get_or_create_team(team1_name, season_node)
        team2_node = get_or_create_team(team2_name, season_node)
        team_nodes = {team1_name: team1_node, team2_name: team2_node}

        # Create Match node and link to Teams
        toss_info = info.get('toss', {})
        toss_winner = toss_info.get('winner')  # e.g., "Royal Challengers Bangalore"
        toss_decision = toss_info.get('decision')  # e.g., "bat" or "field"

        player_of_match = info.get('player_of_match', [])  # List of players
        # Ensure it's a list
        if isinstance(player_of_match, str):
            player_of_match = [player_of_match]

        outcome = info.get('outcome', {})
        winner = outcome.get('winner')
        eliminator = outcome.get('eliminator')
        by_runs = outcome.get('by', {}).get('runs')
        by_wickets = outcome.get('by', {}).get('wickets')

        if winner:
            winner_str = winner
            if by_runs:
                result = f"{winner} won by {by_runs} runs"
            elif by_wickets:
                result = f"{winner} won by {by_wickets} wickets"
            else:
                result = f"{winner} won"
        elif eliminator:
            winner_str = eliminator
            result = f"{eliminator} won by super over"
            season_stats[season_year]["super_over_matches"] += 1  # Increment super over counter
        else:
            winner_str = None
            result = outcome.get('result', 'No result')

        match_properties = {
            "match_id": match_id,
            "date": match_date,
            "season": season_year,
            "tournament": info.get('event', {}).get('name', league_name),
            "match_number": match_number,
            "stage": info.get('event', {}).get('stage'),
            "venue": info.get('venue'),
            "city": info.get('city'),
            "match_type": info.get('match_type'),
            "gender": info.get('gender'),
            "total_overs": info.get('overs'),
            "balls_per_over": info.get('balls_per_over'),
            "toss_winner": toss_winner,
            "toss_decision": toss_decision,
            "player_of_match": player_of_match,
            "result": result,
            "winner": winner or eliminator,
            "had_super_over": bool(eliminator),
            "data_version": meta.get('data_version'),
            "created": meta.get('created'),
            "revision": meta.get('revision'),
            # Add the new playoffs property
            "playoffs": info.get('event', {}).get('stage', '').lower() != 'group stage'
        }

        # Create Match node
        match_node = Node("Match", match_id=match_id)
        match_node.update(match_properties)
        graph.merge(match_node, "Match", "match_id")

        # Add Duckworth-Lewis property
        if "method" in info["outcome"] and info["outcome"]["method"] == "D/L":
            match_node["duckworth_lewis"] = True
        else:
            match_node["duckworth_lewis"] = False

        # Add more properties to enhance the schema
        match_node["city"] = info.get("city", "Unknown")
        match_node["venue"] = info.get("venue", "Unknown")
        match_node["player_of_match"] = ", ".join(info.get("player_of_match", []))
        match_node["toss_winner"] = info["toss"]["winner"]
        match_node["toss_decision"] = info["toss"]["decision"]

        # Link Teams to Match
        rel1 = Relationship(team1_node, "PLAYED_IN", match_node)
        rel2 = Relationship(team2_node, "PLAYED_IN", match_node)
        graph.merge(rel1)
        graph.merge(rel2)
        logging.info(f"Linked Teams {team1_name} and {team2_name} to Match {match_id}")

        # Link Match to Venue
        venue_name = info.get('venue')
        city = info.get('city')
        venue_node = get_or_create_venue(venue_name, city)
        rel = Relationship(match_node, "PLAYED_AT", venue_node)
        graph.merge(rel)
        logging.info(f"Linked Match {match_id} to Venue {venue_name}")

        # Create Officials
        officials = info.get('officials', {})
        for role, official_names in officials.items():
            for official_name in official_names:
                official_node = get_or_create_official(official_name, role)
                # Define relationship type based on role
                if role == "umpires":
                    rel_type = "HAS_UMPIRE"
                elif role == "match_referees":
                    rel_type = "HAS_MATCH_REFEREE"
                elif role == "reserve_umpires":
                    rel_type = "HAS_RESERVE_UMPIRE"
                elif role == "tv_umpires":
                    rel_type = "HAS_TV_UMPIRE"
                else:
                    rel_type = "HAS_OFFICIAL"
                rel = Relationship(match_node, rel_type, official_node)
                graph.merge(rel)
                logging.info(f"Linked Match {match_id} to Official {official_name} as {rel_type}.")

        # Create Outcome
        if winner_str:
            # Update the Match node
            match_node["winner"] = winner_str
            if by_runs is not None:
                match_node["won_by_runs"] = int(by_runs)
            if by_wickets is not None:
                match_node["won_by_wickets"] = int(by_wickets)
            
            graph.push(match_node)
            logging.info(f"Updated Match {match_id} with winner: {winner_str}")

            # Link Match to Winning Team
            winning_team_node = team_nodes.get(winner_str)
            if winning_team_node:
                rel = Relationship(match_node, "WON_BY", winning_team_node)
                graph.merge(rel)
                logging.info(f"Linked Match {match_id} to Winning Team: {winner_str}.")
            else:
                logging.warning(f"Winning team '{winner_str}' not found in team_nodes.")

        # Process Players
        players_info = info.get('players', {})
        registry = info.get('registry', {}).get('people', {})
        player_nodes = {}

        for team_name, players in players_info.items():
            team_node = team_nodes.get(team_name)
            if not team_node:
                logging.warning(f"Team {team_name} not found in team_nodes.")
                continue
            for player_name in players:
                registry_id = registry.get(player_name)
                if not registry_id:
                    logging.warning(f"No registry ID for player '{player_name}' in file {file}. Skipping player.")
                    continue
                player_node = get_or_create_player(player_name, registry_id)
                player_nodes[player_name] = player_node
                # Link Player to Team
                rel = Relationship(player_node, "BELONGS_TO", team_node)
                graph.merge(rel)
                logging.info(f"Linked Player {player_name} to Team {team_name}")

        # ------------------------ Process Innings, Overs, Deliveries ------------------------

        # Initialize player statistics
        player_stats = defaultdict(lambda: {
            "runs": 0,
            "balls_faced": 0,
            "runs_conceded": 0,
            "balls_bowled": 0,
            "wickets": 0,
            "fours": 0,
            "sixes": 0,
            "catches": 0,
            "run_outs": 0,
            "stumpings": 0
        })

        # Update season statistics
        season_stats[season_year]["total_matches"] += 1

        # Initialize a flag for D/L method
        is_dl_method = False

        for i, innings_data in enumerate(innings_list):
            logging.info(f"Inning {i+1} data: {innings_data}")
            # Check if innings_data is a dict with a single key like '1st innings'
            if isinstance(innings_data, dict):
                if len(innings_data) == 1 and isinstance(next(iter(innings_data.values())), dict):
                    # Old format, extract the innings data
                    innings_name = next(iter(innings_data))
                    innings = innings_data[innings_name]
                elif 'team' in innings_data:
                    # New format, innings_data is the innings dict
                    innings = innings_data
                else:
                    logging.error(f"Invalid innings_data structure: {innings_data}")
                    continue
            else:
                logging.error(f"Invalid innings_data type: {type(innings_data)}")
                continue

            team_name = innings.get('team')
            if not team_name:
                logging.warning(f"Missing team name in innings in file {file}. Skipping innings.")
                continue

            team_node = team_nodes.get(team_name)
            if not team_node:
                logging.warning(f"Team {team_name} not found in team_nodes.")
                continue

            # Extract overs list and calculate total overs
            overs_list = innings.get('overs', [])
            total_overs_in_innings = len(overs_list)

            # Calculate total runs and initialize over number
            runs = 0
            over_number = -1

            # Check if this is a super over
            is_super_over = innings.get('super_over', False)

            # Create a unique key for the innings
            innings_key = f"{match_id}_{i+1}_{team_name}_{'super_over' if is_super_over else 'regular'}"

            # Create Innings node
            innings_node = Node("Innings",
                                innings_key=innings_key,
                                team=team_name,
                                runs=0,  # Will update later
                                total_overs=0,  # Will update later
                                innings_number=i + 1,
                                match_id=match_id,
                                is_super_over=is_super_over)
            graph.merge(innings_node, "Innings", "innings_key")
            logging.info(f"Created/Merged {'Super Over' if is_super_over else 'Regular'} Innings {i + 1} for Team {team_name} in Match {match_id}.")

            # Link Match to Innings
            rel = Relationship(match_node, "HAS_INNINGS", innings_node)
            graph.merge(rel)

            # Initialize phase statistics
            phase_stats = {
                "Powerplay": {"runs": 0, "balls": 0, "wickets": 0},
                "Middle Overs": {"runs": 0, "balls": 0, "wickets": 0},
                "Death Overs": {"runs": 0, "balls": 0, "wickets": 0}
            }

            # Process Overs and Deliveries
            for over_data in overs_list:
                over_number = over_data.get('over')
                if over_number is None:
                    logging.warning(f"Missing over number in file {file}. Skipping over.")
                    continue

                # Adjust over_number to start from 1
                over_number += 1

                # Create a unique key for the over
                over_key = f"{match_id}_{i+1}_{over_number}_{team_name}"

                over_node = Node("Over",
                                 over_key=over_key,
                                 number=over_number,
                                 innings_number=i + 1,
                                 match_id=match_id,
                                 team=team_name)
                graph.merge(over_node, "Over", "over_key")
                logging.info(f"Created/Merged Over {over_number} for Innings {i + 1}.")

                # Link Innings to Over
                rel = Relationship(innings_node, "HAS_OVER", over_node)
                graph.merge(rel)

                deliveries = over_data.get('deliveries', [])
                legal_ball_in_over = 0
                current_ball_number = 1

                for delivery_index, delivery_data in enumerate(deliveries):
                    # Extract delivery information
                    runs_batter = delivery_data.get("runs", {}).get("batter", 0)
                    runs_extras = delivery_data.get("runs", {}).get("extras", 0)
                    total_runs_delivery = delivery_data.get("runs", {}).get("total", 0)
                    extras_type = delivery_data.get("extras", {})

                    is_legal = "wides" not in extras_type and "noballs" not in extras_type
                    is_leg_bye = "legbyes" in extras_type
                    is_bye = "byes" in extras_type
                    is_wicket = "wickets" in delivery_data

                    # Only update phase statistics for regular innings, not for super overs
                    if not innings.get('super_over', False):
                        # Determine the phase of the game
                        phase = get_phase(over_number, current_ball_number)

                        # Update phase statistics
                        phase_stats[phase]["runs"] += total_runs_delivery
                        if is_legal:
                            phase_stats[phase]["balls"] += 1
                        if is_wicket:
                            phase_stats[phase]["wickets"] += 1

                    # Adjust ball_number to reflect the correct over
                    ball_number = f"{over_number-1}.{current_ball_number}"

                    # Create a unique key for the delivery
                    delivery_key = f"{match_id}_{i+1}_{ball_number}_{delivery_index}"

                    # Determine delivery type
                    delivery_type = "regular"
                    if not is_legal:
                        if "wides" in extras_type:
                            delivery_type = "wide"
                        elif "noballs" in extras_type:
                            delivery_type = "no_ball"
                    else:
                        if "legbyes" in extras_type:
                            delivery_type = "leg_bye"
                        elif "byes" in extras_type:
                            delivery_type = "bye"

                    # Handle Wickets
                    if is_wicket:
                        wickets = delivery_data.get("wickets", [])
                        if wickets:
                            wicket = wickets[0]  # Assume one wicket per delivery
                            wicket_type = wicket.get("kind")
                            if wicket_type == "caught":
                                fielders = wicket.get("fielders", [])
                                if fielders:
                                    fielder_name = fielders[0].get("name")  # Get the name of the first fielder

                    # Create Delivery node
                    delivery_node = Node("Delivery",
                                         delivery_key=delivery_key,
                                         ball_number=ball_number,
                                         delivery_index=delivery_index + 1,
                                         runs_batter=runs_batter,
                                         runs_extras=runs_extras,
                                         total_runs=total_runs_delivery,
                                         is_wicket=is_wicket,
                                         is_legal=is_legal,
                                         over_number=over_number,
                                         legal_ball_in_over=legal_ball_in_over + 1,
                                         innings_number=i + 1,
                                         match_id=match_id,
                                         phase=phase,
                                         batsman=delivery_data.get('batter'),
                                         bowler=delivery_data.get('bowler'),
                                         non_striker=delivery_data.get('non_striker'),
                                         delivery_type=delivery_type)
                    graph.merge(delivery_node, "Delivery", "delivery_key")
                    logging.info(f"Created/Merged Delivery {ball_number} in Over {over_number}.")

                    # Link Over to Delivery
                    rel = Relationship(over_node, "HAS_DELIVERY", delivery_node)
                    graph.merge(rel)

                    # Retrieve Bowler, Batter, and Non-Striker Nodes
                    bowler_name = delivery_data.get('bowler')
                    bowler_node = player_nodes.get(bowler_name)
                    if not bowler_node:
                        logging.warning(f"Bowler '{bowler_name}' not found in file {file}. Skipping delivery.")
                        continue

                    batter_name = delivery_data.get('batter')
                    batter_node = player_nodes.get(batter_name)
                    if not batter_node:
                        logging.warning(f"Batter '{batter_name}' not found in file {file}. Skipping delivery.")
                        continue

                    non_striker_name = delivery_data.get('non_striker')
                    non_striker_node = player_nodes.get(non_striker_name)
                    if not non_striker_node:
                        logging.warning(f"Non-Striker '{non_striker_name}' not found in file {file}. Skipping delivery.")
                        continue

                    runs += total_runs_delivery  # Update total runs

                    # Update player statistics
                    # Batter
                    player_stats[batter_name]["balls_faced"] += 1  # Count all deliveries, including no-balls
                    player_stats[batter_name]["runs"] += runs_batter
                    if runs_batter == 4:
                        player_stats[batter_name]["fours"] += 1
                    elif runs_batter == 6:
                        player_stats[batter_name]["sixes"] += 1

                    # Bowler
                    if is_legal:
                        player_stats[bowler_name]["balls_bowled"] += 1
                    else:
                        player_stats[bowler_name]["no_balls"] = player_stats[bowler_name].get("no_balls", 0) + 1

                    # Only add runs to bowler's conceded runs if they're not leg byes or byes
                    bowler_conceded_runs = total_runs_delivery - (extras_type.get("legbyes", 0) + extras_type.get("byes", 0))
                    player_stats[bowler_name]["runs_conceded"] += bowler_conceded_runs

                    # Update wickets
                    if is_wicket:
                        player_stats[bowler_name]["wickets"] += len(delivery_data.get("wickets", []))
                        for wicket in delivery_data.get("wickets", []):
                            kind = wicket.get("kind")
                            player_out = wicket.get("player_out")
                            fielders = [fielder.get("name") for fielder in wicket.get("fielders", [])]

                            # Update fielding stats
                            for fielder_name in fielders:
                                player_stats[fielder_name]["catches"] += 1  # Assuming 'caught' is the only fielding kind

                    # Link Delivery to Bowler
                    rel = Relationship(delivery_node, "BOWLED_BY", bowler_node)
                    graph.merge(rel)

                    # Link Delivery to Batter
                    rel = Relationship(delivery_node, "BATSMAN", batter_node)
                    graph.merge(rel)

                    # Link Delivery to Non-Striker
                    rel = Relationship(delivery_node, "NON_STRIKER", non_striker_node)
                    graph.merge(rel)

                    # Handle Wickets
                    if is_wicket:
                        wickets = delivery_data.get("wickets", [])
                        for wicket in wickets:
                            kind = wicket.get("kind")
                            player_out = wicket.get("player_out")
                            fielders = [fielder.get("name") for fielder in wicket.get("fielders", [])]

                            # Create a unique key for the wicket
                            wicket_key = f"{match_id}_{i+1}_{ball_number}_{player_out}"

                            # Create Wicket node
                            wicket_node = Node("Wicket",
                                               wicket_key=wicket_key,
                                               kind=kind,
                                               player_out=player_out,
                                               ball_number=ball_number,
                                               over_number=over_number,
                                               innings_number=i + 1,
                                               match_id=match_id,
                                               fielders=", ".join(fielders))  # Join fielders into a string
                            graph.merge(wicket_node, "Wicket", "wicket_key")
                            logging.info(f"Created/Merged Wicket: {player_out} was {kind}.")

                            # Link Delivery to Wicket
                            rel = Relationship(delivery_node, "RESULTS_IN", wicket_node)
                            graph.merge(rel)

                            # Link Fielders to Wicket
                            for fielder_name in fielders:
                                fielder_node = player_nodes.get(fielder_name)
                                if fielder_node:
                                    rel = Relationship(wicket_node, "FIELDED_BY", fielder_node)
                                    graph.merge(rel)
                                    logging.info(f"Linked Wicket to Fielder {fielder_name}.")
                                else:
                                    logging.warning(f"Fielder '{fielder_name}' not found for Wicket in Delivery {ball_number}.")

                    # Update ball numbers after processing the delivery
                    if is_legal:
                        legal_ball_in_over += 1
                        current_ball_number += 1
                    # For illegal deliveries, we don't increment current_ball_number

                    if legal_ball_in_over == 6:
                        break  # End the over after 6 legal deliveries

            # Update Innings node with total runs and overs
            innings_node['runs'] = runs
            innings_node['total_overs'] = over_number
            graph.push(innings_node)

            # Update lowest team score only for non-D/L matches and completed innings
            if not is_dl_method and innings_node['total_overs'] > 0 and not innings.get('super_over', False):
                if season_stats[season_year]["lowest_team_score"] is None or runs < season_stats[season_year]["lowest_team_score"]:
                    season_stats[season_year]["lowest_team_score"] = runs

            # Only create Phase nodes for regular innings, not for super overs
            if not innings.get('super_over', False):
                for phase, stats in phase_stats.items():
                    phase_node = Node("Phase",
                                      innings_key=innings_key,
                                      phase=phase,
                                      runs=stats["runs"],
                                      balls=stats["balls"],
                                      wickets=stats["wickets"])
                    graph.merge(phase_node, "Phase", ("innings_key", "phase"))
                    logging.info(f"Created/Updated {phase} for Innings {i + 1}: Runs: {stats['runs']}, Balls: {stats['balls']}, Wickets: {stats['wickets']}")

                    # Link Innings to Phase
                    rel = Relationship(innings_node, "HAS_PHASE", phase_node)
                    graph.merge(rel)
            else:
                logging.info(f"Skipping phase creation for super over in Innings {i + 1}")

            # Update season statistics
            season_stats[season_year]["total_runs"] += runs
            season_stats[season_year]["highest_team_score"] = max(season_stats[season_year]["highest_team_score"], runs)

            for over_data in overs_list:
                for delivery_data in over_data.get('deliveries', []):
                    # Update wickets
                    if "wickets" in delivery_data:
                        season_stats[season_year]["total_wickets"] += len(delivery_data["wickets"])

                    # Update fours and sixes
                    runs_batter = delivery_data.get("runs", {}).get("batter", 0)
                    if runs_batter == 4:
                        season_stats[season_year]["total_fours"] += 1
                    elif runs_batter == 6:
                        season_stats[season_year]["total_sixes"] += 1

            # Add a count of D/L matches to season statistics
            season_stats[season_year]["duckworth_lewis_matches"] = season_stats[season_year].get("duckworth_lewis_matches", 0) + (1 if match_node["duckworth_lewis"] else 0)

            # Update match node to indicate it had a super over
            if is_super_over:
                match_node['had_super_over'] = True
                graph.push(match_node)

        # Update Player Nodes with Statistics
        for player_name, stats in player_stats.items():
            player_node = player_nodes.get(player_name)
            if player_node:
                # Create batting performance
                if stats["balls_faced"] > 0:
                    batting_stats = {
                        "runs": stats["runs"],
                        "balls_faced": stats["balls_faced"],
                        "fours": stats.get("fours", 0),
                        "sixes": stats.get("sixes", 0),
                        "strike_rate": float(stats['runs'] / stats['balls_faced'] * 100) if stats['balls_faced'] > 0 else 0.0
                    }
                    batting_perf = Node("BattingPerformance",
                                        type="Batting",
                                        match_id=match_node['match_id'],
                                        player_id=player_node['registry_id'],
                                        **batting_stats)
                    graph.create(batting_perf)
                    # Relationships
                    rel1 = Relationship(player_node, "BATTED_IN", match_node)
                    rel2 = Relationship(batting_perf, "PERFORMANCE_OF", player_node)
                    rel3 = Relationship(batting_perf, "IN_MATCH", match_node)
                    graph.create(rel1 | rel2 | rel3)

                # Create bowling performance
                if stats["balls_bowled"] > 0:
                    bowling_stats = {
                        "wickets": stats["wickets"],
                        "runs_conceded": stats["runs_conceded"],
                        "balls_bowled": stats["balls_bowled"],
                        "no_balls": stats.get("no_balls", 0),
                        "economy": float(stats['runs_conceded'] / (stats['balls_bowled'] / 6)) if stats['balls_bowled'] > 0 else 0.0
                    }
                    bowling_perf = Node("BowlingPerformance",
                                        type="Bowling",
                                        match_id=match_node['match_id'],
                                        player_id=player_node['registry_id'],
                                        **bowling_stats)
                    graph.create(bowling_perf)
                    # Relationships
                    rel1 = Relationship(player_node, "BOWLED_IN", match_node)
                    rel2 = Relationship(bowling_perf, "PERFORMANCE_OF", player_node)
                    rel3 = Relationship(bowling_perf, "IN_MATCH", match_node)
                    graph.create(rel1 | rel2 | rel3)

                # Create fielding performance
                fielding_stats = {
                    "catches": stats.get("catches", 0),
                    "run_outs": stats.get("run_outs", 0),
                    "stumpings": stats.get("stumpings", 0)
                }
                if any(fielding_stats.values()):
                    fielding_perf = Node("FieldingPerformance",
                                         type="Fielding",
                                         match_id=match_node['match_id'],
                                         player_id=player_node['registry_id'],
                                         **fielding_stats)
                    graph.create(fielding_perf)
                    # Relationships
                    rel1 = Relationship(player_node, "FIELDED_IN", match_node)
                    rel2 = Relationship(fielding_perf, "PERFORMANCE_OF", player_node)
                    rel3 = Relationship(fielding_perf, "IN_MATCH", match_node)
                    graph.create(rel1 | rel2 | rel3)

                logging.info(f"Created performance nodes for Player {player_name}")
            else:
                logging.warning(f"Player '{player_name}' not found in player_nodes.")

        # After processing the match, update the winner if it's the final match
        if info.get('event', {}).get('stage') == 'Final':
            season_stats[season_year]["winner"] = winner

        # Begin a transaction and commit all nodes and relationships
        # Note: Since we are using graph.merge and graph.create throughout, the changes are already committed.
        # If you want to wrap everything in a single transaction, you can use graph.begin() and tx.commit().

    # Use ThreadPoolExecutor to process files in parallel
    max_workers = 8  # Adjust the number of workers as needed
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_file, file): file for file in json_files}
        for future in tqdm(as_completed(futures), total=len(futures), desc="Processing files"):
            file = futures[future]
            try:
                future.result()
            except Exception as exc:
                logging.error(f"File {file} generated an exception: {exc}")

    # After processing all files, update Season nodes
    for season, stats in season_stats.items():
        season_node = get_or_create_season(season, league_node)  # Pass league_node here
        
        # Update season node with calculated statistics
        season_node.update({
            "total_runs": stats["total_runs"],
            "total_wickets": stats["total_wickets"],
            "number_of_matches": stats["total_matches"],
            "highest_team_score": stats["highest_team_score"],
            "lowest_team_score": stats["lowest_team_score"] if stats["lowest_team_score"] is not None else "N/A",
            "most_sixes": stats["total_sixes"],
            "most_fours": stats["total_fours"],
            "format": "T20",  # Assuming all matches are T20
            "number_of_teams": len(stats["teams"]),
            "winner": stats.get("winner", "TO_BE_UPDATED"),  # Use get() with a default value
            "super_over_matches": stats["super_over_matches"],  # New property for super over matches
        })
        graph.push(season_node)
        logging.info(f"Updated Season node for {season} with calculated statistics, including number of super over matches.")

    logging.info("Import process completed.")
    print("Import process completed. Check 'importingg.log' for details.")

# ------------------------ Run the Import ------------------------

def setup_database_schema():
    constraints_and_indexes = [
        "CREATE CONSTRAINT IF NOT EXISTS FOR (l:League) REQUIRE l.name IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (s:Season) REQUIRE s.year IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (t:Team) REQUIRE t.name IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (p:Player) REQUIRE p.registry_id IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (o:Official) REQUIRE o.name IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (v:Venue) REQUIRE v.name IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (m:Match) REQUIRE m.match_id IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (i:Innings) REQUIRE i.innings_key IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (o:Over) REQUIRE o.over_key IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (d:Delivery) REQUIRE d.delivery_key IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (w:Wicket) REQUIRE w.wicket_key IS UNIQUE",
        "CREATE INDEX IF NOT EXISTS FOR (p:Player) ON (p.name)",
        "CREATE INDEX IF NOT EXISTS FOR (m:Match) ON (m.date)",
        "CREATE INDEX IF NOT EXISTS FOR (m:Match) ON (m.season)",
        "CREATE INDEX IF NOT EXISTS FOR (i:Innings) ON (i.team)",
        "CREATE INDEX IF NOT EXISTS FOR (d:Delivery) ON (d.ball_number)",
        "CREATE INDEX IF NOT EXISTS FOR (d:Delivery) ON (d.phase)",
        "CREATE INDEX IF NOT EXISTS FOR (bp:BattingPerformance) ON (bp.match_id, bp.player_id)",
        "CREATE INDEX IF NOT EXISTS FOR (bp:BowlingPerformance) ON (bp.match_id, bp.player_id)",
        "CREATE INDEX IF NOT EXISTS FOR (fp:FieldingPerformance) ON (fp.match_id, fp.player_id)"
    ]

    for statement in constraints_and_indexes:
        try:
            graph.run(statement)
            logging.info(f"Successfully executed: {statement}")
        except Exception as e:
            logging.error(f"Error executing {statement}: {str(e)}")

# Call this function before starting the import process
setup_database_schema()

if __name__ == "__main__":
    for json_dir in JSON_DIRS:
        print(f"Processing directory: {json_dir}")
        import_json_to_neo4j(json_dir, LEAGUE_NAME)
        print(f"Finished processing directory: {json_dir}")
    
    print("All directories processed. Check 'importingg.log' for details.")