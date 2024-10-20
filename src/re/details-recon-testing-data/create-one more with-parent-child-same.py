# Define schema for a simpler nested structure with parent ID in child
schema = StructType([
    StructField("id", IntegerType(), True),  # Parent ID
    StructField("name", StringType(), True),
    StructField("contacts", ArrayType(
        StructType([
            StructField("contact_id", IntegerType(), True),  # Contact primary key
            StructField("id", IntegerType(), True),  # Parent ID within child
            StructField("type", StringType(), True),
            StructField("phone", StringType(), True),
            StructField("email", StringType(), True)
        ])
    ), True)
])

# Define source dataset with parent ID in child
data_source = [
    (1, "Alice", [
        {"contact_id": 101, "id": 1, "type": "home", "phone": "123-456-7890", "email": "alice.home@example.com"},
        {"contact_id": 102, "id": 1, "type": "work", "phone": "234-567-8901", "email": "alice.work@example.com"}
    ]),
    (2, "Bob", [
        {"contact_id": 201, "id": 2, "type": "home", "phone": "345-678-9012", "email": "bob.home@example.com"},
        {"contact_id": 202, "id": 2, "type": "mobile", "phone": "567-890-1234", "email": "bob.mobile@example.com"}
    ]),
    (3, "Charlie", [
        {"contact_id": 301, "id": 3, "type": "work", "phone": "678-901-2345", "email": "charlie.work@example.com"}
    ]),
    (4, "David", [
        {"contact_id": 401, "id": 4, "type": "work", "phone": "789-012-3456", "email": "david.work@example.com"},
        {"contact_id": 402, "id": 4, "type": "mobile", "phone": "890-123-4567", "email": "david.mobile@example.com"}
    ]),
    (5, "Eve", [
        {"contact_id": 501, "id": 5, "type": "mobile", "phone": "901-234-5678", "email": "eve.mobile@example.com"}
    ])
]

# Define target dataset with parent ID in child and some differences
data_target = [
    (1, "Alice", [
        {"contact_id": 101, "id": 1, "type": "home", "phone": "123-456-7890", "email": "alice.newhome@example.com"},  # Modified email
        {"contact_id": 102, "id": 1, "type": "work", "phone": "234-567-8901", "email": "alice.work@example.com"}
    ]),
    (2, "Bob", [
        {"contact_id": 201, "id": 2, "type": "home", "phone": "345-678-9012", "email": "bob.home@example.com"},
        {"contact_id": 202, "id": 2, "type": "mobile", "phone": "567-890-9999", "email": "bob.newmobile@example.com"}  # Modified phone and email
    ]),
    (3, "Charlie", [
        {"contact_id": 301, "id": 3, "type": "work", "phone": "678-901-2345", "email": "charlie.work@example.com"}
    ]),
    (4, "David", [
        {"contact_id": 401, "id": 4, "type": "work", "phone": "789-012-3456", "email": "david.newwork@example.com"},  # Modified email
        {"contact_id": 402, "id": 4, "type": "mobile", "phone": "890-123-4567", "email": "david.mobile@example.com"}
    ]),
    (5, "Eve", [
        {"contact_id": 501, "id": 5, "type": "mobile", "phone": "901-234-5678", "email": "eve.newmobile@example.com"}  # Modified email
    ])
]

# Create source and target DataFrames
df_source = spark.createDataFrame(data_source, schema=schema)
df_target = spark.createDataFrame(data_target, schema=schema)