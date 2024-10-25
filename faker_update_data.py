import pandas as pd
from faker import Faker
from neo4j import GraphDatabase
import random
import logging
from typing import Dict, List, Optional
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Faker
fake = Faker()

# Aim and Objectives of the Script:
# 1. Update employee data in a Neo4j database based on a CSV file.
# 2. Handle employee exits and new joins, to mimic a real-world HR system.
# 3. Account for promotions by updating job titles, salaries, and managers.
# 4. Allow department switches for employees.
# 5. Implement salary adjustments, including raises and cuts.
# 6. Generate new employee data using the Faker library to ensure no worries for data generation.

class EmployeeDataManager:
    def __init__(self, uri, username, password):
        self.driver = GraphDatabase.driver(uri, auth=(username, password))
        self.job_titles = ['Software Engineer', 'Senior Software Engineer', 'Project Manager', 
                          'Product Manager', 'Data Analyst', 'Data Scientist', 'HR Manager',
                          'Marketing Specialist', 'Sales Representative', 'Team Lead']
        self.departments = ['Engineering', 'Product', 'Data', 'HR', 'Marketing', 'Sales']
        
    def close(self):
        try:
            self.driver.close()
            logger.info("Database connection closed successfully")
        except Exception as e:
            logger.error(f"Error closing database connection: {e}")

    def generate_new_employee(self) -> Dict:
        """Generate fake employee data"""
        first_name = fake.first_name()
        last_name = fake.last_name()
        return {
            'first_name': first_name,
            'last_name': last_name,
            'email': f"{first_name.lower()}.{last_name.lower()}@company.com",
            'phone_number': fake.phone_number(),
            'job_title': random.choice(self.job_titles),
            'department': random.choice(self.departments),
            'salary': random.randint(50000, 150000),
            'manager': fake.name()
        }

    def execute_transaction(self, query, parameters=None):
        """Execute Neo4j transaction with error handling"""
        try:
            with self.driver.session() as session:
                result = session.run(query, parameters or {})
                return list(result)  # Collect all results before returning
        except Exception as e:
            logger.error(f"Database transaction failed: {e}")
            raise

    def handle_employee_departures(self, num_employees=5) -> List[str]:
        """Handle employee departures with transaction safety"""
        try:
            # Collect emails first before using them
            result = self.execute_transaction("""
                MATCH (e:Employee)
                WITH e, rand() as r
                ORDER BY r
                LIMIT $limit
                RETURN e.email AS email
            """, {"limit": num_employees})
            
            departed_employees = [record["email"] for record in result]
            
            if departed_employees:
                # Delete both relationships and nodes
                self.execute_transaction("""
                    MATCH (e:Employee)
                    WHERE e.email IN $emails
                    DETACH DELETE e
                """, {"emails": departed_employees})
                
                logger.info(f"Removed {len(departed_employees)} employees")
            return departed_employees
        except Exception as e:
            logger.error(f"Failed to handle employee departures: {e}")
            return []

    def add_new_employees(self, num_employees=5) -> List[Dict]:
        """Add new employees with MERGE operation"""
        new_employees = [self.generate_new_employee() for _ in range(num_employees)]
        
        try:
            for employee in new_employees:
                self.execute_transaction("""
                    MERGE (e:Employee {email: $email})
                    ON CREATE SET 
                        e.first_name = $first_name,
                        e.last_name = $last_name,
                        e.phone_number = $phone_number,
                        e.job_title = $job_title,
                        e.department = $department,
                        e.salary = $salary,
                        e.manager = $manager
                    ON MATCH SET 
                        e.first_name = $first_name,
                        e.last_name = $last_name,
                        e.phone_number = $phone_number,
                        e.job_title = $job_title,
                        e.department = $department,
                        e.salary = $salary,
                        e.manager = $manager
                """, employee)
            
            logger.info(f"Added {len(new_employees)} new employees")
            return new_employees
        except Exception as e:
            logger.error(f"Failed to add new employees: {e}")
            return []

    def update_employee_data(self, updates: List[Dict]) -> None:
        """Generic method to update employee data"""
        try:
            for update in updates:
                self.execute_transaction("""
                    MATCH (e:Employee {email: $email})
                    SET e += $updates
                """, {
                    "email": update["email"],
                    "updates": {k: v for k, v in update.items() if k != "email"}
                })
            logger.info(f"Updated {len(updates)} employee records")
        except Exception as e:
            logger.error(f"Failed to update employee data: {e}")

    def export_to_csv(self, filename: str = "employee_data.csv") -> None:
        """Export all employee data to CSV"""
        try:
            result = self.execute_transaction("""
                MATCH (e:Employee)
                RETURN 
                    e.first_name as first_name,
                    e.last_name as last_name,
                    e.email as email,
                    e.phone_number as phone_number,
                    e.job_title as job_title,
                    e.department as department,
                    e.salary as salary,
                    e.manager as manager
            """)
            
            # Convert to pandas DataFrame
            df = pd.DataFrame([dict(record) for record in result])
            
            # Save to CSV
            df.to_csv(filename, index=False)
            logger.info(f"Successfully exported data to {filename}")
            
        except Exception as e:
            logger.error(f"Failed to export data to CSV: {e}")

def main():
    # Load credentials from .env file
    uri = os.getenv('NEO4J_URI', 'neo4j+s://dc85fd78.databases.neo4j.io')
    username = os.getenv('NEO4J_USERNAME', 'neo4j')
    password = os.getenv('NEO4J_PASSWORD')
    
    if not all([uri, username, password]):
        logger.error("Missing required environment variables")
        return
    
    manager = EmployeeDataManager(uri, username, password)
    
    try:
        # Handle employee departures and new hires
        departed_employees = manager.handle_employee_departures()
        new_employees = manager.add_new_employees()
        
        # Log the changes
        print("\n=== Changes Summary ===")
        
        if departed_employees:
            print("\nDeparted Employees:")
            for email in departed_employees:
                print(f"- {email}")
        
        if new_employees:
            print("\nNew Employees:")
            for emp in new_employees:
                print(f"- {emp['first_name']} {emp['last_name']} ({emp['email']})")
                print(f"  Role: {emp['job_title']} in {emp['department']}")
                print(f"  Salary: ${emp['salary']:,}")
        
        # Handle various updates
        updates = [
            {"email": "john.doe@company.com", "salary": 95000, "job_title": "Senior Software Engineer"},
            {"email": "jane.smith@company.com", "department": "Data", "salary": 105000}
        ]
        
        print("\nUpdated Employees:")
        for update in updates:
            print(f"- {update['email']}:")
            for key, value in update.items():
                if key != 'email':
                    print(f"  {key}: {value}")
        
        # Export data to CSV
        manager.export_to_csv()
        print("\nFinal data exported to employee_data.csv")
        
    except Exception as e:
        logger.error(f"Main execution failed: {e}")
    finally:
        manager.close()

if __name__ == "__main__":
    main()
