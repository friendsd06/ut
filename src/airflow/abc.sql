-- Create the user if it doesn't exist
CREATE USER myuser WITH PASSWORD 'mypassword';

-- Grant privileges to the user
ALTER USER myuser WITH SUPERUSER;
GRANT ALL PRIVILEGES ON DATABASE mydatabase TO myuser;

init-scripts/01-init.sql
./init-scripts:/docker-entrypoint-initdb.d