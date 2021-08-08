# Implementing Data Warehouse On Redshift

>The goal of this project is to extract raw data stored in an S3 Bucket and load them unto staging tables on Redshift 
>for transformation through SQL queries executed on them to create final analytical tables.

## Table of contents

* [General info](#general-info)
* [Screenshots](#screenshots)
* [Technologies and Libraries](#technologies-and-libraries)
* [Project Setup](#project-setup)
* [Contact](#contact)

## General info
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. To better serve customers the start-up wants to better understand how users interact with the app and there's no better way to do it at scale than applying dimensional modelling best practises in a data warehouse.

## Screenshots
Fig.1: Sample Staging_songs table on redshift
![staging_songs_table_ss_redshift](https://user-images.githubusercontent.com/76578061/110278526-a41a3380-7f94-11eb-9dd2-4704831ffc5c.png)

Fig.2: Sample Staging_events table on redshift
![staging_events_table_se_redshift](https://user-images.githubusercontent.com/76578061/110278709-fd826280-7f94-11eb-8f21-414ebe2dd504.png)

Fig.2: Sample songplay(fact table) on redshift
![songplay tabe s redshift](https://user-images.githubusercontent.com/76578061/128617679-e096f353-c14c-46cd-b0bf-3f1da2cf798d.png)

## Technologies and Libraries
* jupyterlap - version 1.0.9
* psycopg2
* configparser

## Project Setup
In this section we'll discuss the different project steps which include: **Create Table Schema** and **Build ETL Pipeline**

### Create Table Schema
This step involves designing the schema for the fact and dimension tables for the data model. There are 3 main python scripts in this step that are worth exploring in detail: `create_table.py`, `sql_queries.py` and `dwh.cfg`.

1. `create_table.py` is a python script that creates the fact and dimensional tables for the star schema in redshift.
2. `sql_queries.py` contains the SQL statements that are to imported into both the `create_table.py` and `sql_queries.py`.
3. `dwh.cfg` contains the configuration for the redshift data warehouse with IAM credentials and connection details.

### Build ETL Pipeline
This step involves implements the the python script in the previous step inorder to extract the data from S3 and ingest into a staging table in the redshift data warehouse where they are later loaded into fact and dimensional tables for analysis. The python script that is responsible for this process is `etl.py`.

## Contact
Created by @[peterndiforchu](https://www.linkedin.com/in/peter-ndiforchu-0b8986129) - feel free to contact me!
