from neo4j import GraphDatabase
import csv
import os
from dotenv import load_dotenv
load_dotenv()
class Neo4jHRSchemaCreator:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def create_node(self, query, parameters):
        with self.driver.session() as session:
            session.run(query, parameters)

    def delete_existing_properties(self, label):
        with self.driver.session() as session:
            session.run(f"MATCH (n:{label}) REMOVE n.location_id, n.department_id, n.role_id, n.manager_id")

    def load_csv_and_create_nodes(self, csv_file, create_query):
        local_csv_path = os.path.join(os.getcwd(), csv_file)
        with open(local_csv_path, newline='') as file:
            reader = csv.DictReader(file)
            for row in reader:
                self.create_node(create_query, row)

    def create_relationship(self, query, parameters):
        with self.driver.session() as session:
            session.run(query, parameters)

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

    def create_location_nodes(self):
        location_create_query = """
        MERGE (l:Location {location_id: $location_id})
        ON CREATE SET l.city = $city, l.country = $country
        """
        self.load_csv_and_create_nodes('locations.csv', location_create_query)

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

        location_relationship_query = """
        MATCH (e:Employee), (l:Location)
        WHERE e.location_id = l.location_id
        MERGE (e)-[:LOCATED_AT]->(l)
        """
        self.create_relationship(location_relationship_query, {})

        manager_relationship_query = """
        MATCH (e:Employee), (m:Employee)
        WHERE e.manager_id = m.employee_id
        MERGE (m)-[:MANAGES]->(e)
        """
        self.create_relationship(manager_relationship_query, {})

    def create_project_skill_training_relationships(self):
        # Create relationships for projects, skills, and trainings by reading employees.csv
        local_csv_path = os.path.join(os.getcwd(), 'employees.csv')
        with open(local_csv_path, newline='') as file:
            reader = csv.DictReader(file)
            for row in reader:
                employee_id = row['employee_id']

                # Create WORKS_ON relationships for projects
                project_ids = row['project_ids'].split(',')
                for project_id in project_ids:
                    project_relationship_query = """
                    MATCH (e:Employee {employee_id: $employee_id}), (p:Project {project_id: $project_id})
                    MERGE (e)-[:WORKS_ON]->(p)
                    """
                    self.create_relationship(project_relationship_query, {'employee_id': employee_id, 'project_id': project_id})

                # Create HAS_SKILL relationships for skills
                skill_ids = row['skill_ids'].split(',')
                for skill_id in skill_ids:
                    skill_relationship_query = """
                    MATCH (e:Employee {employee_id: $employee_id}), (s:Skill {skill_id: $skill_id})
                    MERGE (e)-[:HAS_SKILL]->(s)
                    """
                    self.create_relationship(skill_relationship_query, {'employee_id': employee_id, 'skill_id': skill_id})

                # Create COMPLETED relationships for trainings
                training_ids = row['training_ids'].split(',')
                for training_id in training_ids:
                    training_relationship_query = """
                    MATCH (e:Employee {employee_id: $employee_id}), (t:Training {training_id: $training_id})
                    MERGE (e)-[:COMPLETED]->(t)
                    """
                    self.create_relationship(training_relationship_query, {'employee_id': employee_id, 'training_id': training_id})

    def create_all_nodes_and_relationships(self):
        # Delete existing properties to ensure a fresh start
        self.delete_existing_properties('Employee')
        self.delete_existing_properties('Department')
        self.delete_existing_properties('Role')
        self.delete_existing_properties('Project')
        self.delete_existing_properties('Location')
        self.delete_existing_properties('Skill')
        self.delete_existing_properties('Training')
        self.create_employee_nodes()
        self.create_department_nodes()
        self.create_role_nodes()
        self.create_project_nodes()
        self.create_location_nodes()
        self.create_skill_nodes()
        self.create_training_nodes()
        self.create_relationships_for_employees()
        self.create_project_skill_training_relationships()

if __name__ == "__main__":
    uri = os.getenv("NEO4J_URI")
    user = os.getenv("NEO4J_USERNAME")
    password = os.getenv("NEO4J_PASSWORD")
    creator = Neo4jHRSchemaCreator(uri, user, password)
    creator.create_all_nodes_and_relationships()
    creator.close()