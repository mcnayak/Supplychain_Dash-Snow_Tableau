# supply_chain_dashboard
This Repo provides scripts to create database, load snowflake tables, and download Tableau dashboard

Steps:
1. Run  supply_chain_tables.sql to create a database called supply_db, a public schema and necessary Snowflake Tables in the schema. 
2. The script further creates a storage integration to AWS bucket which has the demo data.
3. Finally, the script loads data from the S3 bucket to Snowflake tables.
4. Download the Tableau Dashboard to your desktop, enter your Snowflake account URL and credentials.
5. Select "Suppy Chain Snowflakes" as the tableau datasource. 
6. Your Supply Chain Dashboard should now be powered by Snowflake data.
7. If you are using Tableau Cloud, upload the workbook there.
