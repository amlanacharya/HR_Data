import csv
import random
from faker import Faker

fake = Faker('en_IN')

# Function to generate consistent and correct data for all nodes
def generate_data():
    num_employees = 50
    num_departments = 5
    num_roles = 10
    num_projects = 15
    num_locations = 5
    num_skills = 20
    num_trainings = 10

    # Create consistent department, role, and project IDs
    department_ids = [f"D{str(i).zfill(3)}" for i in range(1, num_departments + 1)]
    role_ids = [f"R{str(i).zfill(3)}" for i in range(1, num_roles + 1)]
    project_ids = [f"P{str(i).zfill(3)}" for i in range(1, num_projects + 1)]
    location_ids = [f"L{str(i).zfill(3)}" for i in range(1, num_locations + 1)]
    skill_ids = [f"S{str(i).zfill(3)}" for i in range(1, num_skills + 1)]
    training_ids = [f"T{str(i).zfill(3)}" for i in range(1, num_trainings + 1)]

    # Generate Departments Data
    with open('departments.csv', mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['department_id', 'name', 'manager_id'])
        for department_id in department_ids:
            writer.writerow([department_id, fake.bs().title(), None])

    # Generate Roles Data
    with open('roles.csv', mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['role_id', 'name', 'level'])
        for role_id in role_ids:
            writer.writerow([role_id, fake.job(), random.choice(['Junior', 'Mid', 'Senior'])])

    # Generate Projects Data
    with open('projects.csv', mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['project_id', 'name', 'start_date', 'end_date'])
        for project_id in project_ids:
            writer.writerow([project_id, fake.bs().title(), fake.date_between(start_date='-2y', end_date='today'), None])

    # Generate Locations Data
    with open('locations.csv', mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['location_id', 'city', 'country'])
        for location_id in location_ids:
            writer.writerow([location_id, fake.city(), fake.country()])

    # Generate Skills Data
    with open('skills.csv', mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['skill_id', 'name', 'proficiency_level'])
        for skill_id in skill_ids:
            writer.writerow([skill_id, fake.bs().title(), random.choice(['Beginner', 'Intermediate', 'Expert'])])

    # Generate Trainings Data
    with open('trainings.csv', mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['training_id', 'name', 'date', 'description'])
        for training_id in training_ids:
            writer.writerow([training_id, fake.bs().title(), fake.date_between(start_date='-1y', end_date='today'), fake.sentence()])

    # Generate Employees Data
    with open('employees.csv', mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['employee_id', 'name', 'hire_date', 'salary', 'email', 'location_id', 'department_id', 'role_id', 'manager_id', 'project_ids', 'skill_ids', 'training_ids'])
        employee_ids = [f"E{str(i).zfill(3)}" for i in range(1, num_employees + 1)]
        for employee_id in employee_ids:
            manager_id = random.choice(employee_ids) if random.random() < 0.3 else None
            department_id = random.choice(department_ids)
            role_id = random.choice(role_ids)
            location_id = random.choice(location_ids)
            projects = random.sample(project_ids, random.randint(1, 3))
            skills = random.sample(skill_ids, random.randint(1, 5))
            trainings = random.sample(training_ids, random.randint(1, 2))
            writer.writerow([
                employee_id, fake.name(), fake.date_between(start_date='-10y', end_date='today'),
                random.randint(30000, 200000), fake.unique.email(), location_id, department_id, role_id,
                manager_id, ','.join(projects), ','.join(skills), ','.join(trainings)
            ])

if __name__ == "__main__":
    generate_data()
    print("CSV files generated successfully without mismatches.")