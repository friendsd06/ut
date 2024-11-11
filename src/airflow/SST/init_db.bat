@echo off
echo Creating and initializing database...

REM Set your project directory in D drive
set PROJECT_DIR=D:\Airflow-DAG\dag_generator

REM Create tables
sqlite3 "%PROJECT_DIR%\dags.db" ".read %PROJECT_DIR%\examples\sql\init_db.sql"

REM Insert sample data
sqlite3 "%PROJECT_DIR%\dags.db" ".read %PROJECT_DIR%\examples\sql\sample_data.sql"

echo Database initialization complete.
pause