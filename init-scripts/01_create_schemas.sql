-- Create schemas for data lake layers
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- Grant usage to postgres user
GRANT USAGE ON SCHEMA bronze TO postgres;
GRANT USAGE ON SCHEMA silver TO postgres;
GRANT USAGE ON SCHEMA gold TO postgres;

-- Grant create privileges for dbt
GRANT CREATE ON SCHEMA bronze TO postgres;
GRANT CREATE ON SCHEMA silver TO postgres;
GRANT CREATE ON SCHEMA gold TO postgres;
