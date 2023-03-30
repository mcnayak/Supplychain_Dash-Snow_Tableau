# supply_chain_dashboard
Scripts to load snowflake tables, and download Tableau dashboard

Steps:
1. Run  supply_chain_tables.sql to create a database, a public schema and necessary Snowflake Tables in the schema
2. The script further creates a storage integration to AWS bucket with demo data.
3. supply_chain_tables.sql then loads data from the S3 bucket.
3. Download the Tableau Dashboard in your desktop, enter your Snowflake account credential,
4. Your Supply Chain Dashboard should now be powered by Snowflake data.
