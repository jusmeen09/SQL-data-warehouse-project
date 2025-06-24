/*
===========================================================================================
Create Database and schemas
===========================================================================================
Script Purpose:
This script create a new database named 'data warehouse '.
Additionally, the scripts sets up three schemas within the database :'bronze','silver' and 'gold'.

Warning :
     Running this script will drop the entire 'DataWarehouse' database if it  already exists.
     All data in the databse will be permanently deleted. Proceed with caution
     and ensure you have proper backups before running this scripts.
*/

-- Create the data warehouse database
CREATE DATABASE datawarehouse;


-- Step 2: Run the following only after connecting to the datawarehouse
-- You must connect to the 'datawarehouse' database before running the rest

-- create Schemas
create schema bronze;
create schema silver;
create schema gold;
