-- File: init-scripts/01-init.sql

-- Create airflow user
CREATE USER airflow WITH PASSWORD 'airflow';

-- Create airflow database
CREATE DATABASE airflow OWNER airflow;

-- Connect to the airflow database
\c airflow

-- Grant all privileges on all tables in airflow database to airflow user
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO airflow;
GRANT ALL PRIVILEGES ON SCHEMA public TO airflow;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO airflow;

init-scripts/01-init.sql
./init-scripts:/docker-entrypoint-initdb.d


# Check if the user was created
docker exec -it postgres_db psql -U postgres -c "\du"

# Check if the database was created
docker exec -it postgres_db psql -U postgres -c "\l"

# Test connecting as airflow user
docker exec -it postgres_db psql -U airflow -d airflow

AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/dags

- postgres_data:/var/lib/postgresql/data

ALTER TABLE "public"."ab_permission_view" ALTER COLUMN "id" SET DEFAULT nextval('ab_permission_view_id_seq'::regclass);
ALTER TABLE "public"."ab_permission" ALTER COLUMN "id" SET DEFAULT nextval('ab_permission_id_seq'::regclass);
ALTER TABLE "public"."ab_permission_view_role" ALTER COLUMN "id" SET DEFAULT nextval('ab_permission_view_role_id_seq'::regclass);
ALTER TABLE "public"."ab_register_user" ALTER COLUMN "id" SET DEFAULT nextval('ab_register_user_id_seq'::regclass);
ALTER TABLE "public"."ab_role" ALTER COLUMN "id" SET DEFAULT nextval('ab_role_id_seq'::regclass);
ALTER TABLE "public"."ab_user" ALTER COLUMN "id" SET DEFAULT nextval('ab_user_id_seq'::regclass);
ALTER TABLE "public"."ab_user_role" ALTER COLUMN "id" SET DEFAULT nextval('ab_user_role_id_seq'::regclass);
ALTER TABLE "public"."ab_view_menu" ALTER COLUMN "id" SET DEFAULT nextval('ab_view_menu_id_seq'::regclass);