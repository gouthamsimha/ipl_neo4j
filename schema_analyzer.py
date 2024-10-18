from neo4j import GraphDatabase
from collections import defaultdict

class Neo4jSchemaAnalyzer:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def analyze_schema(self):
        with self.driver.session() as session:
            node_schema = self._get_node_schema(session)
            relationship_schema = self._get_relationship_schema(session)
            
            return node_schema, relationship_schema

    def _get_node_schema(self, session):
        result = session.run("""
            MATCH (n)
            WITH labels(n) AS labels, keys(n) AS props
            UNWIND labels AS label
            RETURN label, collect(DISTINCT props) AS properties
        """)
        
        node_schema = {}
        for record in result:
            label = record['label']
            props = set()
            for prop_list in record['properties']:
                props.update(prop_list)
            node_schema[label] = sorted(list(props))
        
        return node_schema

    def _get_relationship_schema(self, session):
        result = session.run("""
            MATCH ()-[r]->()
            WITH type(r) AS type, keys(r) AS props, 
                 startNode(r) AS start, endNode(r) AS end
            RETURN type, collect(DISTINCT props) AS properties, 
                   collect(DISTINCT labels(start)) AS start_labels, 
                   collect(DISTINCT labels(end)) AS end_labels
        """)
        
        rel_schema = {}
        for record in result:
            rel_type = record['type']
            props = set()
            for prop_list in record['properties']:
                props.update(prop_list)
            start_labels = set([label for label_list in record['start_labels'] for label in label_list])
            end_labels = set([label for label_list in record['end_labels'] for label in label_list])
            rel_schema[rel_type] = {
                'properties': sorted(list(props)),
                'start_labels': sorted(list(start_labels)),
                'end_labels': sorted(list(end_labels))
            }
        
        return rel_schema

    def print_schema(self):
        node_schema, relationship_schema = self.analyze_schema()
        
        print("Node Schema:")
        for label, props in node_schema.items():
            print(f"  {label}:")
            for prop in props:
                print(f"    - {prop}")
            print()
        
        print("Relationship Schema:")
        for rel_type, details in relationship_schema.items():
            print(f"  {rel_type}:")
            print(f"    Start Node Labels: {', '.join(details['start_labels'])}")
            print(f"    End Node Labels: {', '.join(details['end_labels'])}")
            print("    Properties:")
            for prop in details['properties']:
                print(f"      - {prop}")
            print()

# Usage
uri = "bolt://localhost:7687"  # Replace with your Neo4j URI
user = "neo4j"  # Replace with your username
password = "Myapple7@"  # Replace with your password

analyzer = Neo4jSchemaAnalyzer(uri, user, password)
analyzer.print_schema()
analyzer.close()
