# Define schema for the deeply nested structure with same 'id' in parent and child
schema = StructType([
    StructField("id", IntegerType(), True),  # Parent ID
    StructField("name", StringType(), True),
    StructField("contact_info", ArrayType(
        StructType([
            StructField("child_id", IntegerType(), True),  # Child primary key
            StructField("id", IntegerType(), True),  # Parent ID in child with the same field name
            StructField("type", StringType(), True),
            StructField("details", StructType([
                StructField("primary", StructType([
                    StructField("phone", StringType(), True),
                    StructField("email", StringType(), True)
                ]), True),
                StructField("secondary", ArrayType(
                    StructType([
                        StructField("phone", StringType(), True),
                        StructField("email", StringType(), True)
                    ])
                ), True)
            ]), True)
        ])
    ), True)
])

# Define source dataset with parent ID in child as 'id'
data_source = [
    (1, "Alice", [
        {"child_id": 101, "id": 1, "type": "home", "details": {
            "primary": {"phone": "123-456-7890", "email": "alice.home@example.com"},
            "secondary": [
                {"phone": "111-222-3333", "email": "alice.sec1@example.com"},
                {"phone": "222-333-4444", "email": "alice.sec2@example.com"}
            ]
        }},
        {"child_id": 102, "id": 1, "type": "work", "details": {
            "primary": {"phone": "234-567-8901", "email": "alice.work@example.com"},
            "secondary": [
                {"phone": "333-444-5555", "email": "alice.sec3@example.com"}
            ]
        }}
    ]),
    (2, "Bob", [
        {"child_id": 201, "id": 2, "type": "home", "details": {
            "primary": {"phone": "345-678-9012", "email": "bob.home@example.com"},
            "secondary": []
        }},
        {"child_id": 202, "id": 2, "type": "mobile", "details": {
            "primary": {"phone": "567-890-1234", "email": "bob.mobile@example.com"},
            "secondary": [
                {"phone": "444-555-6666", "email": "bob.sec1@example.com"}
            ]
        }}
    ]),
    (3, "Charlie", [
        {"child_id": 301, "id": 3, "type": "work", "details": {
            "primary": {"phone": "678-901-2345", "email": "charlie.work@example.com"},
            "secondary": [
                {"phone": "555-666-7777", "email": "charlie.sec1@example.com"},
                {"phone": "666-777-8888", "email": "charlie.sec2@example.com"}
            ]
        }}
    ]),
    (4, "David", [
        {"child_id": 401, "id": 4, "type": "work", "details": {
            "primary": {"phone": "789-012-3456", "email": "david.work@example.com"},
            "secondary": []
        }},
        {"child_id": 402, "id": 4, "type": "mobile", "details": {
            "primary": {"phone": "890-123-4567", "email": "david.mobile@example.com"},
            "secondary": [
                {"phone": "777-888-9999", "email": "david.sec1@example.com"}
            ]
        }}
    ]),
    (5, "Eve", [
        {"child_id": 501, "id": 5, "type": "mobile", "details": {
            "primary": {"phone": "901-234-5678", "email": "eve.mobile@example.com"},
            "secondary": [
                {"phone": "888-999-0000", "email": "eve.sec1@example.com"},
                {"phone": "999-000-1111", "email": "eve.sec2@example.com"}
            ]
        }}
    ])
]

# Define target dataset with parent ID in child as 'id' and some differences
data_target = [
    (1, "Alice", [
        {"child_id": 101, "id": 1, "type": "home", "details": {
            "primary": {"phone": "123-456-7890", "email": "alice.newhome@example.com"},  # Modified email
            "secondary": [
                {"phone": "111-222-3333", "email": "alice.sec1@example.com"}
            ]
        }},
        {"child_id": 102, "id": 1, "type": "work", "details": {
            "primary": {"phone": "234-567-8901", "email": "alice.work@example.com"},
            "secondary": [
                {"phone": "999-888-7777", "email": "alice.sec3@example.com"}  # Modified phone
            ]
        }}
    ]),
    (2, "Bob", [
        {"child_id": 201, "id": 2, "type": "home", "details": {
            "primary": {"phone": "345-678-9012", "email": "bob.home@example.com"},
            "secondary": []
        }},
        {"child_id": 202, "id": 2, "type": "mobile", "details": {
            "primary": {"phone": "567-890-1234", "email": "bob.mobile@example.com"},
            "secondary": []  # Removed secondary contact
        }}
    ]),
    (3, "Charlie", [
        {"child_id": 301, "id": 3, "type": "work", "details": {
            "primary": {"phone": "678-901-2345", "email": "charlie.work@example.com"},
            "secondary": [
                {"phone": "555-666-7777", "email": "charlie.sec1@example.com"}
            ]  # Removed one secondary contact
        }}
    ]),
    (4, "David", [
        {"child_id": 401, "id": 4, "type": "work", "details": {
            "primary": {"phone": "789-012-3456", "email": "david.newwork@example.com"},  # Modified email
            "secondary": []
        }},
        {"child_id": 402, "id": 4, "type": "mobile", "details": {
            "primary": {"phone": "890-123-4567", "email": "david.mobile@example.com"},
            "secondary": [
                {"phone": "111-222-3333", "email": "david.newsec1@example.com"}  # Modified email
            ]
        }}
    ]),
    (5, "Eve", [
        {"child_id": 501, "id": 5, "type": "mobile", "details": {
            "primary": {"phone": "901-234-5678", "email": "eve.newmobile@example.com"},  # Modified email
            "secondary": [
                {"phone": "888-999-0000", "email": "eve.sec1@example.com"}
            ]
        }}
    ])
]

# Create source and target DataFrames
df_source = spark.createDataFrame(data_source, schema=schema)
df_target = spark.createDataFrame(data_target, schema=schema)