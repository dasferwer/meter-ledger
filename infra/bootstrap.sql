CREATE USER billing_owner WITH PASSWORD 'billing_owner';
CREATE USER billing WITH PASSWORD 'billing';
CREATE DATABASE billing OWNER billing_owner;
REVOKE ALL ON DATABASE billing FROM PUBLIC;
GRANT CONNECT ON DATABASE billing TO billing;
