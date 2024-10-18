from py2neo import Graph

# Neo4j connection details (update these as needed)
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "Myapple7@"

# Connect to Neo4j
graph = Graph(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

# Cypher query for partnerships
query = """
MATCH (m:Match)-[:HAS_INNINGS]->(i:Innings)-[:HAS_OVER]->(o:Over)-[:HAS_DELIVERY]->(d:Delivery)
WITH m, i, d.batsman AS batsman, d.non_batsman AS non_batsman,
     SUM(d.runs_off_bat + d.extras) AS partnership_runs
WHERE batsman < non_batsman  // Ensure unique partnerships
RETURN 
    m.match_id AS match_id,
    i.team AS batting_team,
    batsman,
    non_batsman,
    partnership_runs
ORDER BY match_id, partnership_runs DESC
"""

# Execute the query
results = graph.run(query)

# Print the results
print("Match ID | Batting Team | Batsman | Non-Batsman | Partnership Runs")
print("-" * 75)
for record in results:
    print(f"{record['match_id']} | {record['batting_team']} | {record['batsman']} | {record['non_batsman']} | {record['partnership_runs']}")
