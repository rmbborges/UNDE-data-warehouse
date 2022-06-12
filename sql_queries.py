import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN = config.get('IAM_ROLE', 'ARN')
LOG_DATA = config.get('S3', 'LOG_DATA')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA = config.get('S3', 'SONG_DATA')

# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"

# CREATE TABLES

staging_events_table_create= ("""
  CREATE TABLE IF NOT EXISTS staging_events (
        eventId BIGINT IDENTITY(0, 1) NOT NULL,
        artist VARCHAR NULL,
        auth VARCHAR  NULL,
        firstName VARCHAR NULL,
        gender VARCHAR NULL,
        itemInSession VARCHAR NULL,
        lastName VARCHAR NULL,
        length VARCHAR NULL,
        level VARCHAR NULL,
        location VARCHAR NULL,
        method VARCHAR NULL,
        page VARCHAR NULL,
        registration VARCHAR NULL,
        sessionId INTEGER NOT NULL,
        song VARCHAR NULL,
        status INTEGER NULL,
        ts BIGINT NOT NULL,
        userAgent VARCHAR NULL,
        userId INTEGER NULL
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs INTEGER NULL,
        artist_id VARCHAR NOT NULL,
        artist_latitude VARCHAR NULL,
        artist_longitude VARCHAR NULL,
        artist_location VARCHAR NULL,
        artist_name VARCHAR NULL,
        song_id VARCHAR NOT NULL,
        title VARCHAR NULL,
        duration DECIMAL NULL,
        year INTEGER NULL
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id INT IDENTITY(0, 1) NOT NULL, 
        start_time TIMESTAMP NULL, 
        user_id INT NOT NULL, 
        level VARCHAR NULL, 
        song_id VARCHAR, 
        artist_id VARCHAR DISTKEY, 
        session_id VARCHAR NULL, 
        location VARCHAR NULL, 
        user_agent VARCHAR NULL
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id INT PRIMARY KEY , 
        first_name VARCHAR NOT NULL, 
        last_name VARCHAR NOT NULL, 
        gender CHAR, 
        level VARCHAR
    );
""")

song_table_create = (""" 
    CREATE TABLE IF NOT EXISTS songs (
        song_id VARCHAR PRIMARY KEY,
        title VARCHAR NOT NULL,
        artist_id VARCHAR NOT NULL DISTKEY, 
        year INT, 
        duration NUMERIC
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id VARCHAR PRIMARY KEY DISTKEY, 
        name VARCHAR NOT NULL, 
        location VARCHAR, 
        latitude NUMERIC,
        longitude NUMERIC
    );
""")

time_table_create = (""" 
    CREATE TABLE IF NOT EXISTS time (
        start_time TIMESTAMP PRIMARY KEY, 
        hour INT, 
        day INT, 
        week INT,
        month INT,
        year INT,
        weekday INT
    );
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM {}
    credentials 'aws_iam_role={}'
    format as json {}
    region 'us-west-2';
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    credentials 'aws_iam_role={}'
    format as json 'auto'
    region 'us-west-2';
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""
     INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
         SELECT
             DISTINCT 
                 TIMESTAMP 
                     'epoch' 
                     + (ts/1000) 
                     * INTERVAL '1 second' 
                         AS start_time,
                 userId AS user_id,
                 level,
                 song_id,
                 artist_id,
                 sessionId AS session_id,
                 location,
                 userAgent AS user_agent
        FROM 
            staging_events
        INNER JOIN 
            staging_songs ON staging_events.artist = staging_songs.artist_name
        WHERE 
            staging_events.page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO users(user_id, first_name, last_name, gender, level)
        SELECT
            DISTINCT 
                userId AS user_id,
                firstName AS first_name,
                lastName AS last_name,
                gender AS gender,
                level AS level
        FROM 
            staging_events
        WHERE
            page = 'NextSong';
""")

song_table_insert = ("""
    INSERT INTO songs(song_id, title, artist_id, year, duration)
        SELECT
            DISTINCT
                song_id,
                title,
                artist_id,
                year,
                duration
        FROM
            staging_songs;
""")

artist_table_insert = ("""
    INSERT INTO artists(artist_id, name, location, latitude, longitude)
        SELECT
            DISTINCT
                artist_id,
                artist_name AS name,
                artist_location AS location,
                artist_latitude AS latitude,
                artist_longitude AS longitude
        FROM
            staging_songs;
""")

time_table_insert = ("""
    INSERT INTO time(start_time, hour, day, week, month, year, weekday)
        SELECT
            DISTINCT
                TIMESTAMP 
                     'epoch' 
                     + (ts/1000) 
                     * INTERVAL '1 second' 
                         AS start_time,
                EXTRACT(hour FROM start_time) AS hour,
                EXTRACT(day FROM start_time) AS day,
                EXTRACT(week FROM start_time) AS week,
                EXTRACT(month FROM start_time) AS month,
                EXTRACT(year FROM start_time) AS year,
                DATE_PART(dow, start_time) AS weekday
        FROM
            staging_events
        WHERE
            page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
