## Project: Data Warehouse
### Introduction
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights into what songs their users are listening to. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

### Project Description
In this project, you'll apply what you've learned on data warehouses and AWS to build an ETL pipeline for a database hosted on Redshift. To complete the project, you will need to load data from S3 to staging tables on Redshift and execute SQL statements that create the analytics tables from these staging tables.

### Project Solution
This project was built based on a two-steps etl pipeline:

```
**Step I**: S3 (json files) >> Redshift (raw)
**Step II**: Redshift (raw) >> Redshift (dw)
``` 

In **Step I**, this ETL process the sparkify public stored files in the S3 bucket bellow:

```
LOG_DATA: 's3://udacity-dend/log_data'
LOG_JSONPATH: 's3://udacity-dend/log_json_path.json'
SONG_DATA: 's3://udacity-dend/song_data'
```

To do so, this ETL execute a `COPY` command, taking advantage of Amazon Redshift parallel data load, and create the staging tables: *staging_events* and *staging_songs*
Due to the small amount of data, the distribution style chose for the staging tables was `AUTO` (i.e., the default distribution spec of Redshift)  

In **Step II**, the ETL process the staging tables of **Step I** to create the data warehouse tables: *time, artists, songs, users* and *songplays*.
For this step, the distribution style chose was based on artist `KEY`, due to the fact that *artists*, *songs* and *songplays* tables, all of them have the `artist_id` column.

### How-to execute this ETL
First, install the project dependencies:
```pip3 install -r requirements.txt```

Second, update the AWS credentials in `dwh.cfg` file.

Third, execute the create_tables script:
```python3 create_tables.py```

At least, execute the following command:
```python3 etl.py```