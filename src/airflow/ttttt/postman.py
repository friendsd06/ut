# First get the token
curl -X POST \
        -H "Content-Type: application/json" \
           -d '{"username":"your_username", "password":"your_password"}' \
              "http://your-airflow-host:8080/api/v1/security/login"

# Then trigger the DAG
curl -X POST \
        -H "Authorization: Bearer your_token" \
           -H "Content-Type: application/json" \
              -d '{
"conf": {
    "data_date": "2024-01-01",
    "parameters": {
        "source": "external_system",
        "process_type": "full_load",
        "email_notification": "user@example.com"
    }
}
}' \
     "http://your-airflow-host:8080/api/v1/dags/example_external_trigger_dag/dagRuns"