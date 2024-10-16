from neo4j import GraphDatabase
import os
import csv
from dotenv import load_dotenv
load_dotenv()
class Neo4jHRSchemaCreator:
    def __init__(self,uri,user,password):
        self.driver=GraphDatabase.driver(uri,auth=(user,password))
    def close(self):
        self.driver.close()
    def create_node(self,query,parameters):
        with self.driver.session() as session:
            session.run(query,parameters)
    def load_csv_and_create_nodes(self,csv_file,create_query):
        local_csv_path=os.path.join(os.getcwd(),csv_file)
        with open(local_csv_path,newline='') as file:
            reader=csv.DictReader(file)
            for row in reader:
                self.create_node(create_query,row)
    def create_relationship(self,query,parameters):
        with self.driver.session() as session:
            session.run(query,parameters)
    def modify_relationship_query_with_csv(self):
        csv_file = input("Please enter the CSV file path for modifications: ")
        proceed_all = input("Do you want to proceed with all updates in the CSV file without further confirmation? (yes/no): ")
        proceed_all = proceed_all.lower() == 'yes'

        local_csv_path = csv_file
        with open(local_csv_path, newline='') as file:
            reader = csv.DictReader(file)
            for row in reader:
                node_type = row['node_type']
                match_property = row['match_property']
                match_value = row['match_value']
                property_to_update = row['property_to_update']
                new_value = row['new_value']

                match_query = f"MATCH (n:{node_type} {{{match_property}: $value}})"
                print(f"Generated MATCH query: {match_query}")
                if proceed_all or input("Do you want to proceed with this MATCH query? (yes/no): ").lower() == 'yes':
                    set_query = f"SET n.{property_to_update} = $new_value"
                    print(f"Generated UPDATE query: {set_query}")
                    if proceed_all or input("Do you want to proceed with this UPDATE query? (yes/no): ").lower() == 'yes':
                        parameters = {
                            "value": match_value,
                            "new_value": new_value
                        }
                        with self.driver.session() as session:
                            session.run(match_query, parameters)
                            session.run(set_query, parameters)

    def create_employee_nodes(self):
        employee_create_query = """
        MERGE (e:Employee {employee_id: $employee_id})
        ON CREATE SET e.name = $name, e.hire_date = $hire_date, e.salary = toInteger($salary),
        e.email = $email, e.location_id = $location_id, e.department_id = $department_id, e.role_id = $role_id,
        e.manager_id = $manager_id
        """
        self.load_csv_and_create_nodes('employees.csv', employee_create_query)

    def create_department_nodes(self):
        department_create_query = """
        MERGE (d:Department {department_id: $department_id})
        ON CREATE SET d.name = $name, d.manager_id = $manager_id
        """
        self.load_csv_and_create_nodes('departments.csv', department_create_query)

    def create_role_nodes(self):
        role_create_query = """
        MERGE (r:Role {role_id: $role_id})
        ON CREATE SET r.name = $name, r.level = $level
        """
        self.load_csv_and_create_nodes('roles.csv', role_create_query)

    def create_project_nodes(self):
        project_create_query = """
        MERGE (p:Project {project_id: $project_id})
        ON CREATE SET p.name = $name, p.start_date = $start_date, p.end_date = $end_date
        """
        self.load_csv_and_create_nodes('projects.csv', project_create_query)

    def create_country_and_city_nodes(self):
        country_create_query = """
        MERGE (c:Country {name: $country})
        """
        self.load_csv_and_create_nodes('locations.csv', country_create_query)

        city_create_query = """
        MERGE (ci:City {name: $city})
        ON CREATE SET ci.location_id = $location_id
        """
        self.load_csv_and_create_nodes('locations.csv', city_create_query)

        city_country_relationship_query = """
        MATCH (ci:City), (c:Country)
        WHERE ci.name = $city AND c.name = $country
        MERGE (ci)-[:LOCATED_IN]->(c)
        """
        self.load_csv_and_create_nodes('locations.csv', city_country_relationship_query)

    def create_skill_nodes(self):
        skill_create_query = """
        MERGE (s:Skill {skill_id: $skill_id})
        ON CREATE SET s.name = $name, s.proficiency_level = $proficiency_level
        """
        self.load_csv_and_create_nodes('skills.csv', skill_create_query)

    def create_training_nodes(self):
        training_create_query = """
        MERGE (t:Training {training_id: $training_id})
        ON CREATE SET t.name = $name, t.date = $date, t.description = $description
        """
        self.load_csv_and_create_nodes('trainings.csv', training_create_query)

    def create_relationships_for_employees(self):
        department_relationship_query = """
        MATCH (e:Employee), (d:Department)
        WHERE e.department_id = d.department_id
        MERGE (e)-[:BELONGS_TO]->(d)
        """
        self.create_relationship(department_relationship_query, {})

        role_relationship_query = """
        MATCH (e:Employee), (r:Role)
        WHERE e.role_id = r.role_id
        MERGE (e)-[:HAS_ROLE]->(r)
        """
        self.create_relationship(role_relationship_query, {})

        located_at_city_query = """
        MATCH (e:Employee), (ci:City)
        WHERE e.location_id = ci.location_id
        MERGE (e)-[:LOCATED_AT]->(ci)
        """
        self.create_relationship(located_at_city_query, {})

        manager_relationship_query = """
        MATCH (e:Employee), (m:Employee)
        WHERE e.manager_id = m.employee_id
        MERGE (m)-[:MANAGES]->(e)
        """
        self.create_relationship(manager_relationship_query, {})

        reports_to_relationship_query = """
        MATCH (e:Employee), (m:Employee)
        WHERE e.manager_id = m.employee_id
        MERGE (e)-[:REPORTS_TO]->(m)
        """
        self.create_relationship(reports_to_relationship_query, {})

        project_relationship_query = """
        MATCH (e:Employee), (p:Project)
        WHERE e.project_id CONTAINS p.project_id
        MERGE (e)-[:WORKS_ON]->(p)
        """
        self.create_relationship(project_relationship_query, {})

    def create_project_skill_training_relationships(self):
        local_csv_path = os.path.join(os.getcwd(), 'employees.csv')
        with open(local_csv_path, newline='') as file:
            reader = csv.DictReader(file)
            for row in reader:
                employee_id = row['employee_id']

                project_ids = row['project_ids'].split(',')
                for project_id in project_ids:
                    project_relationship_query = """
                    MATCH (e:Employee {employee_id: $employee_id}), (p:Project {project_id: $project_id})
                    MERGE (e)-[:WORKS_ON]->(p)
                    """
                    self.create_relationship(project_relationship_query, {'employee_id': employee_id, 'project_id': project_id})

                skill_ids = row['skill_ids'].split(',')
                for skill_id in skill_ids:
                    skill_relationship_query = """
                    MATCH (e:Employee {employee_id: $employee_id}), (s:Skill {skill_id: $skill_id})
                    MERGE (e)-[:HAS_SKILL]->(s)
                    """
                    self.create_relationship(skill_relationship_query, {'employee_id': employee_id, 'skill_id': skill_id})

                training_ids = row['training_ids'].split(',')
                for training_id in training_ids:
                    training_relationship_query = """
                    MATCH (e:Employee {employee_id: $employee_id}), (t:Training {training_id: $training_id})
                    MERGE (e)-[:COMPLETED]->(t)
                    """
                    self.create_relationship(training_relationship_query, {'employee_id': employee_id, 'training_id': training_id})

    def create_all_nodes_and_relationships(self):

        self.create_employee_nodes()
        self.create_department_nodes()
        self.create_role_nodes()
        self.create_project_nodes()
        self.create_country_and_city_nodes()
        self.create_skill_nodes()
        self.create_training_nodes()
        self.create_relationships_for_employees()
        self.create_project_skill_training_relationships()
        
        
if __name__ == "__main__":
    uri = os.getenv("NEO4J_URI")
    user = os.getenv("NEO4J_USERNAME")
    password = os.getenv("NEO4J_PASSWORD")

    print(f"URI: {uri}")
    print(f"User: {user}")
    print(f"Password: {password}")

    creator = Neo4jHRSchemaCreator(uri, user, password)
    creator.create_all_nodes_and_relationships()
    creator.close()
