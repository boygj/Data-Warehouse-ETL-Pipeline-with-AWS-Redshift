import configparser

"""
Main function for data warehouse setup.

1. Establishes a connection to the Redshift cluster using configuration parameters.
2. Acquires a cursor object for executing SQL commands.
3. Drops existing tables (if they exist) to reset the database.
4. Creates the necessary staging and analytics tables for the data pipeline.
5. Closes the database connection.

Note:  The connection parameters are read from the 'dwh.cfg' configuration file.
"""

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""

CREATE TABLE IF NOT EXISTS staging_events (
    artist varchar, 
    auth varchar, 
    firstName varchar, 
    gender varchar, 
    itemInSession integer, 
    lastName varchar, 
    length float, 
    level varchar, 
    location varchar, 
    method varchar, 
    page varchar, 
    registration bigint, 
    sessionId integer, 
    song varchar, 
    status integer, 
    ts timestamp, 
    userAgent varchar, 
    userId integer
    );
""")

staging_songs_table_create = ("""

CREATE TABLE IF NOT EXISTS staging_songs( 
    num_songs integer, 
    artist_id varchar, 
    artist_latitude FLOAT, 
    artist_longitude FLOAT, 
    artist_location varchar, 
    artist_name varchar, 
    song_id varchar, 
    title varchar, 
    duration float, 
    year integer
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays ( 
    songplay_id INTEGER IDENTITY(0,1) SORTKEY DISTKEY,
    start_time TIMESTAMP NOT NULL,
    user_id INTEGER NOT NULL,
    level VARCHAR,
    song_id VARCHAR NOT NULL,
    artist_id VARCHAR NOT NULL,
    session_id INTEGER,
    location VARCHAR,
    user_agent VARCHAR
);
""")

user_table_create = ("""

CREATE TABLE IF NOT EXISTS users(
    user_id integer not null sortkey, 
    first_name varchar, 
    last_name varchar, 
    gender varchar, 
    level varchar
);
""")

song_table_create = ("""

CREATE TABLE IF NOT EXISTS songs(
    song_id varchar PRIMARY KEY, 
    title varchar, 
    artist_id varchar, 
    year integer, 
    duration float
);
""")

artist_table_create = ("""

CREATE TABLE IF NOT EXISTS artists(
    artist_id  varchar PRIMARY KEY,
    name varchar, 
    location varchar, 
    latitude FLOAT, 
    longitude FLOAT
    
);
""")

time_table_create = ("""

CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP  PRIMARY KEY, 
    hour integer, 
    day integer, 
    week integer, 
    month integer, 
    year integer, 
    weekday varchar
);
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events from {} 
CREDENTIALS 'aws_iam_role={}' 
REGION 'us-west-2' 
TIMEFORMAT as 'epochmillisecs'
TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
FORMAT AS JSON {}; 
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'].strip('"\''), config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
copy staging_songs from {} 
CREDENTIALS 'aws_iam_role={}' 
REGION 'us-west-2' 
TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
JSON 'auto';
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'].strip('"\''))


# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT to_timestamp(to_char(se.ts, '9999-99-99 99:99:99'),'YYYY-MM-DD HH24:MI:SS'),
                se.userId as user_id,
                se.level as level,
                ss.song_id as song_id,
                ss.artist_id as artist_id,
                se.sessionId as session_id,
                se.location as location,
                se.userAgent as user_agent
FROM staging_events se
JOIN staging_songs ss ON se.song = ss.title AND se.artist = ss.artist_name
WHERE se.page='NextSong';
""")
 

user_table_insert = ("""

INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT userId as user_id,
                firstName as first_name,
                lastName as last_name,
                gender as gender,
                level as level
FROM staging_events
where userId IS NOT NULL and page='NextSong';
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id as song_id,
                title as title,
                artist_id as artist_id,
                year as year,
                duration as duration
FROM staging_songs
WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT artist_id as artist_id,
                artist_name as name,
                artist_location as location,
                artist_latitude as latitude,
                artist_longitude as longitude
FROM staging_songs
where artist_id IS NOT NULL;
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT distinct ts,
                EXTRACT(hour from ts),
                EXTRACT(day from ts),
                EXTRACT(week from ts),
                EXTRACT(month from ts),
                EXTRACT(year from ts),
                EXTRACT(weekday from ts)
FROM staging_events
WHERE ts IS NOT NULL;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
