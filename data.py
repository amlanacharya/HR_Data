import csv
import random
from faker import Faker

fake = Faker('en_IN')

# Function to generate employee data based on schema constraints
def generate_employee_data(num_employees):
    employees = []
    for _ in range(num_employees):
        employee = {
            'employee_id': fake.unique.bothify(text='E###'),
            'name': fake.name(),
            'hire_date': fake.date_between(start_date='-10y', end_date='today'),
            'salary': random.randint(30000, 200000),
            'email': fake.unique.email(),
            'location_id': fake.unique.bothify(text='L###'),
            'department_id': fake.unique.bothify(text='D###'),
            'role_id': fake.unique.bothify(text='R###'),
            'project_ids': [fake.unique.bothify(text='P###') for _ in range(random.randint(1, 3))],
            'skill_ids': [fake.unique.bothify(text='S###') for _ in range(random.randint(1, 5))],
            'training_ids': [fake.unique.bothify(text='T###') for _ in range(random.randint(1, 2))],
            'manager_id': fake.unique.bothify(text='E###') if random.random() < 0.5 else None
        }
        employees.append(employee)
    return employees

# Function to generate department data
def generate_department_data(num_departments):
    departments = []
    for _ in range(num_departments):
        department = {
            'department_id': fake.unique.bothify(text='D###'),
            'name': fake.bs().title(),
            'manager_id': fake.unique.bothify(text='E###')
        }
        departments.append(department)
    return departments

# Function to generate role data
def generate_role_data(num_roles):
    roles = []
    for _ in range(num_roles):
        role = {
            'role_id': fake.unique.bothify(text='R###'),
            'name': fake.job(),
            'level': random.choice(['Junior', 'Mid', 'Senior'])
        }
        roles.append(role)
    return roles

# Function to generate project data
def generate_project_data(num_projects):
    projects = []
    for _ in range(num_projects):
        project = {
            'project_id': fake.unique.bothify(text='P###'),
            'name': fake.bs().title(),
            'start_date': fake.date_between(start_date='-5y', end_date='today'),
            'end_date': fake.date_between(start_date='today', end_date='+2y') if random.random() < 0.5 else None
        }
        projects.append(project)
    return projects

# Function to generate location data
def generate_location_data(num_locations):
    locations = []
    for _ in range(num_locations):
        location = {
            'location_id': fake.unique.bothify(text='L###'),
            'city': fake.city(),
            'country': fake.country()
        }
        locations.append(location)
    return locations

# Function to generate skill data
def generate_skill_data(num_skills):
    skills = []
    for _ in range(num_skills):
        skill = {
            'skill_id': fake.unique.bothify(text='S###'),
            'name': fake.bs().title(),
            'proficiency_level': random.choice(['Beginner', 'Intermediate', 'Expert'])
        }
        skills.append(skill)
    return skills

# Function to generate training data
def generate_training_data(num_trainings):
    trainings = []
    for _ in range(num_trainings):
        training = {
            'training_id': fake.unique.bothify(text='T###'),
            'name': fake.bs().title(),
            'date': fake.date_between(start_date='-3y', end_date='today'),
            'description': fake.sentence()
        }
        trainings.append(training)
    return trainings

# Function to write data to CSV
def write_to_csv(data, filename):
    with open(filename, mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)

# Main function
def main():
    num_employees = int(input("Enter the number of employees to generate: "))
    num_departments = int(input("Enter the number of departments to generate: "))
    num_roles = int(input("Enter the number of roles to generate: "))
    num_projects = int(input("Enter the number of projects to generate: "))
    num_locations = int(input("Enter the number of locations to generate: "))
    num_skills = int(input("Enter the number of skills to generate: "))
    num_trainings = int(input("Enter the number of trainings to generate: "))

    employees = generate_employee_data(num_employees)
    departments = generate_department_data(num_departments)
    roles = generate_role_data(num_roles)
    projects = generate_project_data(num_projects)
    locations = generate_location_data(num_locations)
    skills = generate_skill_data(num_skills)
    trainings = generate_training_data(num_trainings)

    write_to_csv(employees, 'employees.csv')
    write_to_csv(departments, 'departments.csv')
    write_to_csv(roles, 'roles.csv')
    write_to_csv(projects, 'projects.csv')
    write_to_csv(locations, 'locations.csv')
    write_to_csv(skills, 'skills.csv')
    write_to_csv(trainings, 'trainings.csv')

    print(f"{num_employees} employee records have been generated and saved to 'employees.csv'.")
    print(f"{num_departments} department records have been generated and saved to 'departments.csv'.")
    print(f"{num_roles} role records have been generated and saved to 'roles.csv'.")
    print(f"{num_projects} project records have been generated and saved to 'projects.csv'.")
    print(f"{num_locations} location records have been generated and saved to 'locations.csv'.")
    print(f"{num_skills} skill records have been generated and saved to 'skills.csv'.")
    print(f"{num_trainings} training records have been generated and saved to 'trainings.csv'.")

if __name__ == "__main__":
    main()