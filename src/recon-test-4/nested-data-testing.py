# Define a nested schema
schema = StructType([
    StructField("ID", IntegerType(), True),
    StructField("Name", StringType(), True),
    StructField("Age", IntegerType(), True),
    StructField("Details", StructType([
        StructField("Department", StringType(), True),
        StructField("Salary", FloatType(), True),
        StructField("Projects", ArrayType(StringType()), True)
    ]))
])

# Create sample data for Source DataFrame
source_data = [
    (1, "John Doe", 28, {"Department": "Engineering", "Salary": 50000.0, "Projects": ["Project1", "Project2"]}),
    (2, "Jane Smith", 34, {"Department": "Marketing", "Salary": 60000.0, "Projects": ["Project3"]}),
    (3, "Sam Brown", 25, {"Department": "Sales", "Salary": 45000.0, "Projects": ["Project1", "Project4"]})
]

# Create sample data for Target DataFrame (with differences)
target_data = [
    (1, "John Doe", 28, {"Department": "Engineering", "Salary": 52000.0, "Projects": ["Project1", "Project3"]}),  # Salary and Projects differ
    (2, "Jane Smith", 34, {"Department": "Marketing", "Salary": 60000.0, "Projects": ["Project3"]}),             # No difference
    (4, "Lucy Gray", 29, {"Department": "HR", "Salary": 48000.0, "Projects": ["Project5"]})                      # Extra row
]