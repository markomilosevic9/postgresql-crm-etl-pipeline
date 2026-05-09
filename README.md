<a href="#"><p align="left">
<img src="https://github.com/onemarc/tech-icons/blob/main/icons/docker-dark.svg" width="50">
<img src="https://github.com/onemarc/tech-icons/blob/main/icons/postgressql-dark.svg" width="50">
</p></a>

# Goal
This project implements a PostgreSQL ETL pipeline for processing mock CRM-like data (customers, plans, subscriptions, and transactions) with the goal of analyzing sales performance. The codebase covers including schema setup, data validation, deduplication, and loading from source CRM tables into dimensional models and fact tables. The project also includes data quality checks and issue logging.

## Important aspects of pipeline implementation
- Uses separate raw, clean, dimensional, mart, and ETL-control schemas
- Supports incremental batch processing with idempotent PostgreSQL upsert patterns
- Follows a Kimball dimensional modelling principles with 2 fact tables: subscription lifecycle facts and transaction-level payment facts
- SCD1 and SCD2 dimensions are implemented
- Builds the sales performance data mart as a materialized view using SCD2-pinned customer attributes and actual transaction dates
- Maintains a persistent data quality log to track issues across pipeline runs


The pipeline logic is placed within few .sql scripts meant to be executed one-by-one:

```bash
└───sql
        sql/schema_init.sql - creates all schemas, tables, and the mart view
        sql/first_mock_data_insert.sql - loads initial batch into raw layer
        sql/etl_pipeline.sql - main data processing script
        sql/second_mock_data_incremental_insert.sql - loads additional data batch (UPDATEs of existing records + new inserts)
```
Note that after loading of incremental mock data sample, the re-execution of `etl_pipeline.sql` script is required.

## Running the pipeline

After cloning the repository or downloading files, you can run the pipeline using Docker. To do so, you can execute following commands in sequence.

From the project root directory:

```bash
docker-compose up -d
```
Then:

```bash
docker exec -i pg-storage psql -U storage -d project_data < sql/schema_init.sql
docker exec -i pg-storage psql -U storage -d project_data < sql/first_mock_data_insert.sql
docker exec -i pg-storage psql -U storage -d project_data < sql/etl_pipeline.sql
docker exec -i pg-storage psql -U storage -d project_data < sql/second_mock_data_incremental_insert.sql
docker exec -i pg-storage psql -U storage -d project_data < sql/etl_pipeline.sql

```

These commands create DB schemas and tables, insert initial mock data sample, run the pipeline, insert incremental mock data sample and re-run the pipeline to simulate the incremental loading. 

Also, after launching Docker container, you can connect to PostgreSQL instance via e.g. DBeaver using following parameters:
```
Host: localhost
Port: 5433
DB: project_data
Username: storage
Password: storage
```

