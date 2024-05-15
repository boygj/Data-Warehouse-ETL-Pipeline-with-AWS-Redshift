# Sparkify Data Warehouse: Song Play Analysis
## Table of Contents

1. [Business Context and Purpose](##1 Business Context and Purpose)
2. [Project Datasets](##2 Project Datasets)
3. [Data Warehouse Schema](##3 Database Schema Design)
4. [ETL Pipeline](##4 ETL Pipeline)
5. [Project Steps](##5 Project Steps)
6. [Running the Project](##6 Running the Project)

## 1. Business Context and Purpose
Sparkify is a rapidly growing music streaming startup. As their user base and song catalog expand, they need to gain deeper insights into user behavior, song popularity, and usage patterns to make informed business decisions. The goal of this data warehouse is to enable Sparkify's analytics team to:

1. Understand User Behavior: Analyze how users interact with the app, what types of songs they prefer, and when they're most active.
2. Identify Popular Songs and Artists: Discover trending songs, top artists, and how preferences change over time.
3. Personalize Recommendations: Use the data to improve song recommendations and user engagement.
4. Optimize Marketing and Content: Inform marketing campaigns and content strategies based on data-driven insights.

In this project, you'll apply what you've learned on data warehouses and AWS to build an ETL pipeline for a database hosted on Redshift. To complete the project, you will need to load data from S3 to staging tables on Redshift and execute SQL statements that create the analytics tables from these staging tables.


## 2. Project Datasets
The data warehouse is built upon two datasets stored in AWS S3:

#### Song Data (s3://udacity-dend/song_data): 
This dataset contains metadata about songs and artists. The files are in JSON format and are partitioned by the first three letters of each song's track ID. Each file includes information such as the number of songs, artist ID, artist location, song ID, title, duration, and year.

#### Log Data (s3://udacity-dend/log_data): 
This dataset consists of log files in JSON format, simulating user activity on a music streaming app. The files are partitioned by year and month. Each event record captures details like the artist, user authentication status, user information, song details, timestamp, user agent, and user ID.

#### Log Metadata (s3://udacity-dend/log_json_path.json): 
This file defines the JSON structure of the log data, guiding Redshift to correctly parse and load the data into staging tables.

## 3. Database Schema Design and ETL Pipeline
### Star Schema Rationale
A star schema was chosen for its simplicity, ease of use for analysis, and performance optimization in Redshift. It consists of a central fact table (songplays) and several dimension tables (users, songs, artists, time). This schema enables efficient querying and aggregation for typical song play analysis questions.

### Fact Table
#### songplays:
Captures events representing a user playing a song.
Key fields: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent.

### Dimension Tables
#### users: Stores information about users.
Key fields: user_id, first_name, last_name, gender, level.
#### songs: Contains details about songs.
Key fields: song_id, title, artist_id, year, duration.
#### artists: Stores information about artists.
Key fields: artist_id, name, location, latitude, longitude.
#### time: Stores time-related details for songplays.
Key fields: start_time, hour, day, week, month, year, weekday.

## 4. ETL Pipeline
1. Staging: Data is copied from S3 (song_data and log_data) into staging tables (staging_events, staging_songs) in Redshift using COPY commands.

2. Transformation: Data is cleaned, transformed, and inserted into the star schema tables from the staging tables.

3. Data Quality Check: The ETL process performs basic checks to ensure data integrity and completeness (counting rows in each table, checking for null values in critical columns, etc.).

## 4. Project Template and Steps
The project template includes four files:

#### create_table.py 
is where you'll create your fact and dimension tables for the star schema in Redshift.
#### etl.py
is where you'll load data from S3 into staging tables on Redshift and then process that data into your analytics tables on Redshift.
#### sql_queries.py
is where you'll define you SQL statements, which will be imported into the two other files above.
#### README.md
is where you'll provide discussion on your process and decisions for this ETL pipeline.


## 5. Project Steps
Below are steps you can follow to complete each component of this project.
#### Create Table Schemas

<ol>
    <li>Design schemas for your fact and dimension tables</li>
<li>Write a SQL CREATE statement for each of these tables in sql_queries.py</li>
<li>Complete the logic in create_tables.py to connect to the database and create these tables</li>
<li>Write SQL DROP statements to drop tables in the beginning of create_tables.py if the tables already exist. This way, you can run create_tables.py whenever you want to reset your database and test your ETL pipeline.</li>
<li>Launch a redshift cluster and create an IAM role that has read access to S3.</li>
<li>Add redshift database and IAM role info to dwh.cfg.</li>
<li>Test by running create_tables.py and checking the table schemas in your redshift database. You can use Query Editor in the AWS Redshift console for this.</li>
</ol>

#### Build ETL Pipeline
<ol>
<li>Implement the logic in etl.py to load data from S3 to staging tables on Redshift.</li>
<li>Implement the logic in etl.py to load data from staging tables to analytics tables on Redshift.</li>
<li>Test by running etl.py after running create_tables.py and running the analytic queries on your Redshift database to compare your results with the expected results.</li>
<li>Delete your redshift cluster when finished.</li>
</ol>

## 6. Running the Project
### Prerequisites:
1. AWS credentials and a Redshift cluster configured.
2. Data uploaded to the S3 buckets specified in dwh.cfg.

### Execution:
1. Run create_tables.py to set up the database schema.
2. Run etl.py to execute the ETL pipeline.

## Data Quality Checks and Analysis
Data Quality: Basic checks are implemented to ensure data completeness and consistency.

Example Queries:

#### Top 10 Songs: 
SELECT s.title, COUNT(*) AS play_count
FROM songplays sp
JOIN songs s ON sp.song_id = s.song_id
GROUP BY s.title
ORDER BY play_count DESC
LIMIT 10;


#### Peak hours:
SELECT t.hour, COUNT(*) AS play_count
FROM songplays sp
JOIN time t ON sp.start_time = t.start_time
GROUP BY t.hour
ORDER BY play_count DESC;