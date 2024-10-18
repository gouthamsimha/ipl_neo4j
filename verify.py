import json
import glob
import os
import logging
from py2neo import Graph, Node, Relationship, Subgraph
from py2neo.matching import NodeMatcher
from tqdm import tqdm
from collections import defaultdict
from decimal import Decimal
from concurrent.futures import ThreadPoolExecutor, as_completed

# ------------------------ Configuration ------------------------

NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "Myapple7@"

JSON_DIRS = [
    "/Users/goutham/ipl_neo4j/data/ipl_matches/S17-2024",
    # ... (other directories)
]

TOURNAMENT_NAME = "Indian Premier League"

logging.basicConfig(filename='importing.log', filemode='w', format='%(asctime)s %(levelname)s:%(message)s', level=logging.INFO)

# ------------------------ Connect to Neo4j ------------------------

try:
    graph = Graph(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    matcher = NodeMatcher(graph)
    logging.info("Successfully connected to Neo4j.")
except Exception as e:
    logging.error(f"Failed to connect to Neo4j: {e}")
    raise e

# ------------------------ Helper Functions ------------------------

def get_or_create_tournament(name, properties=None):
    tournament = Node("Tournament", name=name)
    if properties:
        tournament.update(properties)
    graph.merge(tournament, "Tournament", "name")
    logging.info(f"Created/Merged Tournament: {name}")
    return tournament

def get_or_create_season(year, tournament_node, properties=None):
    season = Node("Season", year=year)
    if properties:
        season.update(properties)
    graph.merge(season, "Season", "year")
    rel = Relationship(tournament_node, "HAS_SEASON", season)
    graph.merge(rel)
    logging.info(f"Created/Merged Season: {year} and linked to Tournament: {tournament_node['name']}")
    return season

def get_or_create_team(name, tournament_node, properties=None):
    team = Node("Team", name=name)
    if properties:
        team.update(properties)
    graph.merge(team, "Team", "name")
    rel = Relationship(team, "PARTICIPATES_IN", tournament_node)
    graph.merge(rel)
    logging.info(f"Created/Merged Team: {name} and linked to Tournament: {tournament_node['name']}")
    return team

def get_or_create_player(name, registry_id, team_node):
    player = Node("Player", registry_id=registry_id, name=name)
    graph.merge(player, "Player", "registry_id")
    rel_team = Relationship(player, "PLAYS_FOR", team_node)
    rel_has = Relationship(team_node, "HAS_PLAYER", player)
    graph.merge(rel_team | rel_has)
    logging.info(f"Created/Merged Player: {name} and linked to Team: {team_node['name']}")
    return player

def get_or_create_official(name, role):
    official = Node("Official", name=name, role=role)
    graph.merge(official, "Official", "name")
    logging.info(f"Created/Merged Official: {name} with role: {role}")
    return official

def get_or_create_venue(name, city):
    venue = Node("Venue", name=name, city=city)
    graph.merge(venue, "Venue", "name")
    logging.info(f"Created/Merged Venue: {name} in city: {city}")
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

def import_json_to_neo4j(json_directory, tournament_name):
    json_files = glob.glob(os.path.join(json_directory, "*.json"))
    logging.info(f"Found {len(json_files)} JSON files to import.")

    if not json_files:
        logging.warning("No JSON files found to import.")
        return

    # Extract tournament properties from the first file
    with open(json_files[0], 'r') as f:
        data = json.load(f)
        info = data.get('info', {})
        event = info.get('event', {})

    tournament_properties = {
        "name": tournament_name,
        "country": "India",
        "format": "T20",
        "gender": info.get('gender', 'male'),
        "tournament": event.get('name', tournament_name),
        "match_type": info.get('match_type', 'T20'),
        "overs": info.get('overs', 20),
        "balls_per_over": info.get('balls_per_over', 6),
        "governing_body": "BCCI",
        "founded": 2007,
        "inaugural_season": 2008,
        "logo": "https://www.iplt20.com/assets/images/ipl-logo.png",
        "website": "https://www.iplt20.com"
    }

    tournament_node = get_or_create_tournament(tournament_name, properties=tournament_properties)

    season_stats = defaultdict(lambda: {
        "total_runs": 0,
        "total_wickets": 0,
        "total_matches": 0,
        "highest_team_score": 0,
        "lowest_team_score": None,
        "total_sixes": 0,
        "total_fours": 0,
        "teams": set(),
        "super_over_matches": 0,
    })

    def process_file(file):
        try:
            with open(file, 'r') as f:
                data = json.load(f)
            logging.info(f"Loaded JSON file: {file}")
        except json.JSONDecodeError as e:
            logging.error(f"JSON decode error in file {file}: {e}")
            return

        meta = data.get('meta', {})
        info = data.get('info', {})
        innings_list = data.get('innings', [])

        match_number = info.get('event', {}).get('match_number')
        match_date = info.get('dates', [None])[0]
        if not match_number or not match_date:
            logging.error(f"Missing match_number or date in file {file}. Skipping.")
            return
        match_id = f"{match_number}_{match_date}"

        season_year = info.get('season')
        if not season_year:
            logging.error(f"Missing season in file {file}. Skipping.")
            return

        season_node = get_or_create_season(season_year, tournament_node)

        teams = info.get('teams', [])
        if len(teams) != 2:
            logging.error(f"Invalid number of teams in file {file}. Skipping.")
            return
        team1_name, team2_name = teams

        season_stats[season_year]["teams"].add(team1_name)
        season_stats[season_year]["teams"].add(team2_name)

        team1_node = get_or_create_team(team1_name, tournament_node)
        team2_node = get_or_create_team(team2_name, tournament_node)
        team_nodes = {team1_name: team1_node, team2_name: team2_node}

        toss_info = info.get('toss', {})
        toss_winner = toss_info.get('winner')
        toss_decision = toss_info.get('decision')

        player_of_match = info.get('player_of_match', [])
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
            season_stats[season_year]["super_over_matches"] += 1
        else:
            winner_str = None
            result = outcome.get('result', 'No result')

        match_properties = {
            "match_id": match_id,
            "date": match_date,
            "season": season_year,
            "tournament": info.get('event', {}).get('name', tournament_name),
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
            "playoffs": info.get('event', {}).get('stage', '').lower() != 'group stage',
            "duckworth_lewis": "method" in info["outcome"] and info["outcome"]["method"] == "D/L"
        }

        match_node = Node("Match", match_id=match_id)
        match_node.update(match_properties)
        graph.merge(match_node, "Match", "match_id")

        rel1 = Relationship(season_node, "HAS_MATCH", match_node)
        graph.merge(rel1)

        venue_name = info.get('venue')
        city = info.get('city')
        venue_node = get_or_create_venue(venue_name, city)
        rel = Relationship(match_node, "PLAYED_AT", venue_node)
        graph.merge(rel)

        officials = info.get('officials', {})
        for role, official_names in officials.items():
            for official_name in official_names:
                official_node = get_or_create_official(official_name, role)
                rel = Relationship(match_node, "OFFICIATED_BY", official_node)
                graph.merge(rel)

        for team_name, team_node in team_nodes.items():
            rel = Relationship(team_node, "PLAYED_IN", match_node)
            graph.merge(rel)

        if winner_str:
            match_node["winner"] = winner_str
            if by_runs is not None:
                match_node["won_by_runs"] = int(by_runs)
            if by_wickets is not None:
                match_node["won_by_wickets"] = int(by_wickets)
            graph.push(match_node)

            winning_team_node = team_nodes.get(winner_str)
            if winning_team_node:
                rel = Relationship(match_node, "WON_BY", winning_team_node)
                graph.merge(rel)

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
                player_node = get_or_create_player(player_name, registry_id, team_node)
                player_nodes[player_name] = player_node

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

        season_stats[season_year]["total_matches"] += 1

        for i, innings_data in enumerate(innings_list):
            if isinstance(innings_data, dict) and len(innings_data) == 1:
                innings_name = next(iter(innings_data))
                innings = innings_data[innings_name]
            elif isinstance(innings_data, dict) and 'team' in innings_data:
                innings = innings_data
            else:
                logging.error(f"Invalid innings_data structure: {innings_data}")
                continue

            team_name = innings.get('team')
            if not team_name:
                logging.warning(f"Missing team name in innings in file {file}. Skipping innings.")
                continue

            team_node = team_nodes.get(team_name)
            if not team_node:
                logging.warning(f"Team {team_name} not found in team_nodes.")
                continue

            overs_list = innings.get('overs', [])
            total_overs_in_innings = len(overs_list)

            runs = 0
            over_number = -1

            is_super_over = innings.get('super_over', False)

            innings_key = f"{match_id}_{i+1}_{team_name}_{'super_over' if is_super_over else 'regular'}"

            innings_node = Node("Innings",
                                innings_key=innings_key,
                                team=team_name,
                                runs=0,
                                total_overs=0,
                                innings_number=i + 1,
                                match_id=match_id,
                                is_super_over=is_super_over)
            graph.merge(innings_node, "Innings", "innings_key")

            rel = Relationship(match_node, "HAS_INNINGS", innings_node)
            graph.merge(rel)

            phase_stats = {
                "Powerplay": {"runs": 0, "balls": 0, "wickets": 0},
                "Middle Overs": {"runs": 0, "balls": 0, "wickets": 0},
                "Death Overs": {"runs": 0, "balls": 0, "wickets": 0}
            }

            for over_data in overs_list:
                over_number = over_data.get('over')
                if over_number is None:
                    logging.warning(f"Missing over number in file {file}. Skipping over.")
                    continue

                over_number += 1

                over_key = f"{match_id}_{i+1}_{over_number}_{team_name}"

                over_node = Node("Over",
                                 over_key=over_key,
                                 number=over_number,
                                 innings_number=i + 1,
                                 match_id=match_id,
                                 team=team_name)
                graph.merge(over_node, "Over", "over_key")

                rel = Relationship(innings_node, "HAS_OVER", over_node)
                graph.merge(rel)

                deliveries = over_data.get('deliveries', [])
                legal_ball_in_over = 0
                current_ball_number = 1

                for delivery_index, delivery_data in enumerate(deliveries):
                    runs_batter = delivery_data.get("runs", {}).get("batter", 0)
                    runs_extras = delivery_data.get("runs", {}).get("extras", 0)
                    total_runs_delivery = delivery_data.get("runs", {}).get("total", 0)
                    extras_type = delivery_data.get("extras", {})

                    is_legal = "wides" not in extras_type and "noballs" not in extras_type
                    is_leg_bye = "legbyes" in extras_type
                    is_bye = "byes" in extras_type
                    is_wicket = "wickets" in delivery_data

                    if not is_super_over:
                        phase = get_phase(over_number, current_ball_number)
                        phase_stats[phase]["runs"] += total_runs_delivery
                        if is_legal:
                            phase_stats[phase]["balls"] += 1
                        if is_wicket:
                            phase_stats[phase]["wickets"] += 1

                    ball_number = f"{over_number-1}.{current_ball_number}"

                    delivery_key = f"{match_id}_{i+1}_{ball_number}_{delivery_index}"

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

                    if is_wicket:
                        wickets = delivery_data.get("wickets", [])
                        if wickets:
                            wicket = wickets[0]
                            wicket_type = wicket.get("kind")
                            if wicket_type == "caught":
                                fielders = wicket.get("fielders", [])
                                if fielders:
                                    fielder_name = fielders[0].get("name")

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

                    rel = Relationship(over_node, "HAS_DELIVERY", delivery_node)
                    graph.merge(rel)

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

                    runs += total_runs_delivery

                    player_stats[batter_name]["balls_faced"] += 1
                    player_stats[batter_name]["runs"] += runs_batter
                    if runs_batter == 4:
                        player_stats[batter_name]["fours"] += 1
                    elif runs_batter == 6:
                        player_stats[batter_name]["sixes"] += 1

                    if is_legal:
                        player_stats[bowler_name]["balls_bowled"] += 1
                    else:
                        player_stats[bowler_name]["no_balls"] = player_stats[bowler_name].get("no_balls", 0) + 1

                    bowler_conceded_runs = total_runs_delivery - (extras_type.get("legbyes", 0) + extras_type.get("byes", 0))
                    player_stats[bowler_name]["runs_conceded"] += bowler_conceded_runs

                    if is_wicket:
                        player_stats[bowler_name]["wickets"] += len(delivery_data.get("wickets", []))
                        for wicket in delivery_data.get("wickets", []):
                            kind = wicket.get("kind")
                            player_out = wicket.get("player_out")
                            fielders = [fielder.get("name") for fielder in wicket.get("fielders", [])]

                            for fielder_name in fielders:
                                player_stats[fielder_name]["catches"] += 1

                    rel = Relationship(delivery_node, "BOWLED_BY", bowler_node)
                    graph.merge(rel)

                    rel = Relationship(delivery_node, "BATTED_BY", batter_node)
                    graph.merge(rel)

                    if is_wicket:
                        wickets = delivery_data.get("wickets", [])
                        for wicket in wickets:
                            kind = wicket.get("kind")
                            player_out = wicket.get("player_out")
                            fielders = [fielder.get("name") for fielder in wicket.get("fielders", [])]

                            wicket_key = f"{match_id}_{i+1}_{ball_number}_{player_out}"

                            wicket_node = Node("Dismissal",
                                               wicket_key=wicket_key,
                                               kind=kind,
                                               player_out=player_out,
                                               ball_number=ball_number,
                                               over_number=over_number,
                                               innings_number=i + 1,
                                               match_id=match_id,
                                               fielders=", ".join(fielders))
                            graph.merge(wicket_node, "Dismissal", "wicket_key")

                            rel = Relationship(delivery_node, "RESULTS_IN", wicket_node)
                            graph.merge(rel)

                            for fielder_name in fielders:
                                fielder_node = player_nodes.get(fielder_name)
                                if fielder_node:
                                    rel = Relationship(wicket_node, "FIELDED_BY", fielder_node)
                                    graph.merge(rel)

                    if is_legal:
                        legal_ball_in_over += 1
                        current_ball_number += 1

                    if legal_ball_in_over == 6:
                        break

            innings_node['runs'] = runs
            innings_node['total_overs'] = over_number
            graph.push(innings_node)

            if not match_node["duckworth_lewis"] and innings_node['total_overs'] > 0 and not is_super_over:
                if season_stats[season_year]["lowest_team_score"] is None or runs < season_stats[season_year]["lowest_team_score"]:
                    season_stats[season_year]["lowest_team_score"] = runs

            if not is_super_over:
                for phase, stats in phase_stats.items():
                    phase_node = Node("Phase",
                                      innings_key=innings_key,
                                      phase=phase,
                                      runs=stats["runs"],
                                      balls=stats["balls"],
                                      wickets=stats["wickets"])
                    graph.merge(phase_node, "Phase", ("innings_key", "phase"))

                    rel = Relationship(innings_node, "HAS_PHASE", phase_node)
                    graph.merge(rel)

            season_stats[season_year]["total_runs"] += runs
            season_stats[season_year]["highest_team_score"] = max(season_stats[season_year]["highest_team_score"], runs)

            for over_data in overs_list:
                for delivery_data in over_data.get('deliveries', []):
                    if "wickets" in delivery_data:
                        season_stats[season_year]["total_wickets"] += len(delivery_data["wickets"])

                    runs_batter = delivery_data.get("runs", {}).get("batter", 0)
                    if runs_batter == 4:
                        season_stats[season_year]["total_fours"] += 1
                    elif runs_batter == 6:
                        season_stats[season_year]["total_sixes"] += 1

            season_stats[season_year]["duckworth_lewis_matches"] = season_stats[season_year].get("duckworth_lewis_matches", 0) + (1 if match_node["duckworth_lewis"] else 0)

            if is_super_over:
                match_node['had_super_over'] = True
                graph.push(match_node)

        for player_name, stats in player_stats.items():
            player_node = player_nodes.get(player_name)
            if player_node:
                if stats["balls_faced"] > 0:
                    batting_stats = {
                        "runs": stats["runs"],
                        "balls_faced": stats["balls_faced"],
                        "fours": stats.get("fours", 0),
                        "sixes": stats.get("sixes", 0),
                        "strike_rate": float(stats['runs'] / stats['balls_faced'] * 100) if stats['balls_faced'] > 0 else 0.0
                    }
                    batting_perf = Node("PlayerMatchPerformance",
                                        type="Batting",
                                        match_id=match_node['match_id'],
                                        player_id=player_node['registry_id'],
                                        **batting_stats)
                    graph.create(batting_perf)
                    rel1 = Relationship(match_node, "HAS_PLAYER_PERFORMANCE", batting_perf)
                    rel2 = Relationship(batting_perf, "PERFORMANCE_OF", player_node)
                    graph.create(rel1 | rel2)

                if stats["balls_bowled"] > 0:
                    bowling_stats = {
                        "wickets": stats["wickets"],
                        "runs_conceded": stats["runs_conceded"],
                        "balls_bowled": stats["balls_bowled"],
                        "no_balls": stats.get("no_balls", 0),
                        "economy": float(stats['runs_conceded'] / (stats['balls_bowled'] / 6)) if stats['balls_bowled'] > 0 else 0.0
                    }
                    bowling_perf = Node("PlayerMatchPerformance",
                                        type="Bowling",
                                        match_id=match_node['match_id'],
                                        player_id=player_node['registry_id'],
                                        **bowling_stats)
                    graph.create(bowling_perf)
                    rel1 = Relationship(match_node, "HAS_PLAYER_PERFORMANCE", bowling_perf)
                    rel2 = Relationship(bowling_perf, "PERFORMANCE_OF", player_node)
                    graph.create(rel1 | rel2)

                fielding_stats = {
                    "catches": stats.get("catches", 0),
                    "run_outs": stats.get("run_outs", 0),
                    "stumpings": stats.get("stumpings", 0)
                }
                if any(fielding_stats.values()):
                    fielding_perf = Node("PlayerMatchPerformance",
                                         type="Fielding",
                                         match_id=match_node['match_id'],
                                         player_id=player_node['registry_id'],
                                         **fielding_stats)
                    graph.create(fielding_perf)
                    rel1 = Relationship(match_node, "HAS_PLAYER_PERFORMANCE", fielding_perf)
                    rel2 = Relationship(fielding_perf, "PERFORMANCE_OF", player_node)
                    graph.create(rel1 | rel2)

        if info.get('event', {}).get('stage') == 'Final':
            season_stats[season_year]["winner"] = winner

    max_workers = 8
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_file, file): file for file in json_files}
        for future in tqdm(as_completed(futures), total=len(futures), desc="Processing files"):
            file = futures[future]
            try:
                future.result()
            except Exception as exc:
                logging.error(f"File {file} generated an exception: {exc}")

    for season, stats in season_stats.items():
        season_node = get_or_create_season(season, tournament_node)
        
        season_node.update({
            "total_runs": stats["total_runs"],
            "total_wickets": stats["total_wickets"],
            "number_of_matches": stats["total_matches"],
            "highest_team_score": stats["highest_team_score"],
            "lowest_team_score": stats["lowest_team_score"] if stats["lowest_team_score"] is not None else "N/A",
            "most_sixes": stats["total_sixes"],
            "most_fours": stats["total_fours"],
            "format": "T20",
            "number_of_teams": len(stats["teams"]),
            "winner": stats.get("winner", "TO_BE_UPDATED"),
            "super_over_matches": stats["super_over_matches"],
        })
        graph.push(season_node)
        logging.info(f"Updated Season node for {season} with calculated statistics, including number of super over matches.")
        graph.push(season_node)
        logging.info(f"Updated Season node for {season} with calculated statistics, including number of super over matches.")

    logging.info("Finished processing all files and updating season statistics.")

# ------------------------ Main Execution ------------------------

if __name__ == "__main__":
    for json_dir in JSON_DIRS:
        import_json_to_neo4j(json_dir, TOURNAMENT_NAME)
    
    logging.info("Data import completed successfully.")