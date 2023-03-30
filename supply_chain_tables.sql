
use role accountadmin; 
create database suppy_acclerator_db; 
create or replace schema public;
--use database HOL_DB;
--use schema public; 


create or replace storage integration supply_chain_store
  type = external_stage
  storage_provider = s3
  storage_aws_role_arn = 'arn:aws:iam::484577546576:role/chandra_snowflake_role'
  enabled = true
  storage_allowed_locations = ('s3://sfdc-demo-files/supply_chain_accelerator');


CREATE OR REPLACE FILE FORMAT CSV_DATA TYPE = CSV FIELD_DELIMITER = ',' SKIP_HEADER = 1;  

CREATE OR REPLACE STAGE supplychain_load_stage
  URL = 's3://sfdc-demo-files/supply_chain_accelerator/'
  STORAGE_INTEGRATION = supply_chain_store
  FILE_FORMAT = CSV_DATA; 


create or replace TABLE "Bom" (
	"Product Code (BOM)" VARCHAR(16777216),
	"Material Code (BOM)" VARCHAR(16777216)
);
create or replace TABLE "Customers" (
	"Customer Code" NUMBER(38,0),
	"Customer" VARCHAR(16777216),
	"Customer Country" VARCHAR(16777216),
	"Customer City" VARCHAR(16777216)
);
create or replace TABLE "Events" (
	"Event Date" VARCHAR(16777216),
	"Event Risk" VARCHAR(16777216),
	"Event" VARCHAR(16777216),
	"Event Mitigation" VARCHAR(16777216)
);
create or replace TABLE "F_Materials_Inventory" (
	"Inventory Material Code" VARCHAR(64),
	"Material Warehouse Code" NUMBER(38,0),
	"Material Inventory Date" DATE,
	"Material On-Hand Qty" NUMBER(38,0),
	"Material Unit Inventory Price" NUMBER(32,8),
	"Material Inventory Qty OUT" NUMBER(38,0),
	"Material Inventory Qty IN" NUMBER(38,0),
	"Material Safety Stock Qty" NUMBER(38,0),
	"Material Over Stock Qty" NUMBER(38,0),
	"Material Inventory Scenario" VARCHAR(64)
);
create or replace TABLE "F_Materials_Purchases" (
	"PO From Country" VARCHAR(16777216),
	"PO From Latitude" FLOAT,
	"PO To Country" VARCHAR(16777216),
	"Purchase Main Transportation Method" VARCHAR(16777216),
	"PO Number" VARCHAR(16777216),
	"PO Supplier Code" NUMBER(38,0),
	"PO Defect Quantity" NUMBER(38,0),
	"PO Nb Deliveries OTIF" NUMBER(38,0),
	"PO Date" DATE,
	"PO Delivery Time" NUMBER(38,0),
	"PO From Longitude" FLOAT,
	"PO Material Code" VARCHAR(16777216),
	"PO Nb Deliveries" NUMBER(38,0),
	"PO To City" VARCHAR(16777216),
	"PO From City" VARCHAR(16777216),
	"PO Delivery Date" DATE,
	"PO Confirmed Quantity" NUMBER(38,0),
	"PO To Latitude" FLOAT,
	"PO Unit Price" FLOAT,
	"PO Requested Quantity" NUMBER(38,0),
	"PO To Longitude" FLOAT
);
create or replace TABLE "F_Products_Inventory" (
	"Inventory Product Code" VARCHAR(64),
	"Product Warehouse Code" NUMBER(38,0),
	"Product Inventory Date" DATE,
	"Product On-Hand Qty" NUMBER(38,0),
	"Product Unit Inventory Price" NUMBER(32,2),
	"Product Inventory Qty OUT" NUMBER(38,0),
	"Product Inventory Qty IN" NUMBER(38,0),
	"Product Safety Stock Qty" NUMBER(38,0),
	"Product Over Stock Qty" NUMBER(38,0),
	"Product Inventory Scenario" VARCHAR(64)
);
create or replace TABLE "F_Products_Sales" (
	"SO Number" VARCHAR(16777216),
	"SO Date" DATE,
	"SO Product Code" VARCHAR(16777216),
	"SO Customer Code" NUMBER(38,0),
	"Sales Main Transportation Method" VARCHAR(16777216),
	"SO Confirmed Quantity" NUMBER(38,0),
	"SO Requested Quantity" NUMBER(38,0),
	"SO Delivery Time" NUMBER(38,0),
	"SO Nb Deliveries" NUMBER(38,0),
	"SO Nb Deliveries OTIF" NUMBER(38,0),
	"SO Defect Quantity" NUMBER(38,0),
	"SO Unit Price" NUMBER(38,0),
	"SO To Country" VARCHAR(16777216),
	"SO From City" VARCHAR(16777216),
	"SO From Latitude" FLOAT,
	"SO From Longitude" FLOAT,
	"SO From Country" VARCHAR(16777216),
	"SO To City" VARCHAR(16777216),
	"SO To Latitude" FLOAT,
	"SO To Longitude" FLOAT
);
create or replace TABLE "Materials" (
	"Material Code" VARCHAR(16777216),
	"Material" VARCHAR(16777216),
	"Material Type" VARCHAR(16777216),
	"Material UOM" VARCHAR(16777216)
);
create or replace TABLE "Pivot" (
	"Table Names" VARCHAR(16777216),
	"P_SupplierCode" NUMBER(38,0),
	"P_MaterialCode" VARCHAR(16777216),
	"P_Date" DATE,
	"P_WarehouseCode" NUMBER(38,0),
	"P_ProductCode" VARCHAR(16777216),
	"P_CustomerCode" NUMBER(38,0)
);
create or replace TABLE "Products" (
	"Product Code" VARCHAR(16777216),
	"Product" VARCHAR(16777216),
	"Product Category" VARCHAR(16777216),
	"Product UOM" VARCHAR(16777216)
);
create or replace TABLE "Suppliers" (
	"Supplier Code" NUMBER(38,0),
	"Supplier" VARCHAR(16777216),
	"Supplier Type" VARCHAR(16777216),
	"Supplier Country" VARCHAR(16777216),
	"Supplier City" VARCHAR(16777216)
);
create or replace TABLE "Warehouses" (
	"Warehouse Code" NUMBER(38,0),
	"Warehouse Country" VARCHAR(16777216), 
	"Warehouse City" VARCHAR(16777216),
	"Warehouse" VARCHAR(16777216)
);    

 -- describe table bom;
-- COPY INTO bom from @~/staged/bom.csv.gz FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ',' SKIP_HEADER = 1);

ls @supplychain_load_stage;

COPY INTO "bom"  FROM @supplychain_load_stage/bom.gz;

COPY INTO "Customers"
  FROM @supplychain_load_stage/customers.gz  ;


COPY INTO "Events"
  FROM @supplychain_load_stage/events.gz  ;


COPY INTO "F_Materials_Inventory"
  FROM @supplychain_load_stage/F_Materials_Inventory.gz  ;

COPY INTO "F_Materials_Purchases"
  FROM @supplychain_load_stage/F_Materials_Purchases.gz  ;


COPY INTO "F_Products_Inventory"
  FROM @supplychain_load_stage/F_Products_Inventory.gz  ;

COPY INTO "F_Products_Sales"
  FROM @supplychain_load_stage/F_Products_Sales.gz;  

COPY INTO "Materials"
  FROM @supplychain_load_stage/Materials.gz;  

COPY INTO "Pivot"
  FROM @supplychain_load_stage/Pivot.gz;  


COPY INTO "Products"
  FROM @supplychain_load_stage/Products.gz;  

COPY INTO "Suppliers"
  FROM @supplychain_load_stage/Suppliers.gz;    

COPY INTO "Warehouses"
  FROM @supplychain_load_stage/Warehouses.gz;    


