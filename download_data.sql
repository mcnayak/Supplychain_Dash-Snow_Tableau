
USE ROLE ACCOUNTADMIN; 
use DATABASE SUPPLY_ACCLERATOR;
use schema public;
CREATE OR REPLACE FILE FORMAT CSV_DATA TYPE = CSV FIELD_DELIMITER = ','; 

create or replace storage integration supply_chain_store
  type = external_stage
  storage_provider = s3
  storage_aws_role_arn = 'arn:aws:iam::484577546576:role/chandra_snowflake_role'
  enabled = true
  storage_allowed_locations = ('s3://sfdc-demo-files/supply_chain_accelerator');

-- Working command 
-- snowsql -c cnayak -d SUPPLY_ACCELERATOR -s public -r tabrole -D tablename=BOM -o output_file=/tmp/bom.csv -o quiet=true -o friendly=false -o header=false -o output_format=csv

desc integration supply_chain_store;

CREATE OR REPLACE STAGE supplychain_unload_stage
  URL = 's3://sfdc-demo-files/supply_chain_accelerator/'
  STORAGE_INTEGRATION = supply_chain_store
  FILE_FORMAT = CSV_DATA; 



  ls @supplychain_unload_stage;

-- Export 
COPY INTO @supplychain_unload_stage/bom.gz
FROM (select * from bom) 
FILE_FORMAT = CSV_DATA
OVERWRITE=TRUE SINGLE=TRUE HEADER=TRUE;


COPY INTO @supplychain_unload_stage/customers.gz
FROM (select * from "Customers") 
FILE_FORMAT = CSV_DATA
OVERWRITE=TRUE SINGLE=TRUE HEADER=TRUE;


COPY INTO @supplychain_unload_stage/events.gz
FROM (select * from "Events") 
FILE_FORMAT = CSV_DATA
OVERWRITE=TRUE SINGLE=TRUE HEADER=TRUE;

COPY INTO @supplychain_unload_stage/F_Materials_Inventory.gz
FROM (select * from "F_Materials_Inventory") 
FILE_FORMAT = CSV_DATA
OVERWRITE=TRUE SINGLE=TRUE HEADER=TRUE;

COPY INTO @supplychain_unload_stage/F_Materials_Purchases.gz
FROM (select * from "F_Materials_Purchases") 
FILE_FORMAT = CSV_DATA
OVERWRITE=TRUE SINGLE=TRUE HEADER=TRUE;

COPY INTO @supplychain_unload_stage/F_Products_Inventory.gz
FROM (select * from "F_Products_Inventory") 
FILE_FORMAT = CSV_DATA
OVERWRITE=TRUE SINGLE=TRUE HEADER=TRUE;

COPY INTO @supplychain_unload_stage/F_Products_Sales.gz
FROM (select * from "F_Products_Sales") 
FILE_FORMAT = CSV_DATA
OVERWRITE=TRUE SINGLE=TRUE HEADER=TRUE;

COPY INTO @supplychain_unload_stage/Materials.gz
FROM (select * from "Materials") 
FILE_FORMAT = CSV_DATA
OVERWRITE=TRUE SINGLE=TRUE HEADER=TRUE;

COPY INTO @supplychain_unload_stage/Pivot.gz
FROM (select * from "Pivot") 
FILE_FORMAT = CSV_DATA
OVERWRITE=TRUE SINGLE=TRUE HEADER=TRUE;

COPY INTO @supplychain_unload_stage/Products.gz
FROM (select * from "Products") 
FILE_FORMAT = CSV_DATA
OVERWRITE=TRUE SINGLE=TRUE HEADER=TRUE;

COPY INTO @supplychain_unload_stage/Suppliers.gz
FROM (select * from "Suppliers") 
FILE_FORMAT = CSV_DATA
OVERWRITE=TRUE SINGLE=TRUE HEADER=TRUE;

COPY INTO @supplychain_unload_stage/Suppliers.gz
FROM (select * from "Warehouses") 
FILE_FORMAT = CSV_DATA
OVERWRITE=TRUE SINGLE=TRUE HEADER=TRUE;