from neo4j import GraphDatabase

# Database connection details
URI = "bolt://localhost:7687"  # Replace with your Neo4j URI
USERNAME = "neo4j"  # Replace with your username
PASSWORD = "Myapple7@"  # Replace with your password

def empty_database(tx, batch_size=10000):
    # Delete relationships in batches
    while True:
        result = tx.run(f"""
            MATCH ()-[r]->()
            WITH r LIMIT {batch_size}
            DELETE r
            RETURN count(r) as deleted
        """)
        if result.single()["deleted"] == 0:
            break

    # Delete nodes in batches
    while True:
        result = tx.run(f"""
            MATCH (n)
            WITH n LIMIT {batch_size}
            DELETE n
            RETURN count(n) as deleted
        """)
        if result.single()["deleted"] == 0:
            break

def main():
    driver = GraphDatabase.driver(URI, auth=(USERNAME, PASSWORD))
    
    with driver.session() as session:
        session.execute_write(empty_database)
    
    print("Database emptied successfully.")
    driver.close()

if __name__ == "__main__":
    main()
