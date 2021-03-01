import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events";
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs";
songplay_table_drop = "DROP TABLE IF EXISTS songplay";
users_table_drop = "DROP TABLE IF EXISTS users";
song_table_drop = "DROP TABLE IF EXISTS song";
artist_table_drop = "DROP TABLE IF EXISTS artist";
time_table_drop = "DROP TABLE IF EXISTS time";

# CREATE TABLES

staging_events_table_create= (""" 
    create table if not exists staging_events (artist VARCHAR, \
                                               auth VARCHAR, \
                                               firstName VARCHAR, \
                                               gender  VARCHAR, \
                                               iteminSession INTEGER, \
                                               lastName VARCHAR, \
                                               length FLOAT NULL, \
                                               level VARCHAR, \
                                               location VARCHAR, \
                                               method VARCHAR, \
                                               page VARCHAR, \
                                               registration FLOAT, \
                                               sessionid INTEGER, \
                                               song VARCHAR, \
                                               status VARCHAR, \
                                               ts TIMESTAMP, \
                                               userAgent VARCHAR, \
                                               userid VARCHAR);
""")

staging_songs_table_create = ("""
    create table if not exists staging_songs (num_songs INTEGER, \
                                              artist_id VARCHAR, \
                                              artist_latitude FLOAT, \
                                              artist_longitude FLOAT, \
                                              artist_location VARCHAR, \
                                              artist_name VARCHAR, \
                                              song_id VARCHAR, \
                                              title VARCHAR, \
                                              duration FLOAT, \
                                              year INTEGER);
""")

songplay_table_create = (""" 
    create table if not exists songplay (songplay_id BIGINT IDENTITY(0,1) PRIMARY KEY, \
                                         start_time BIGINT NOT NULL, \
                                         user_id VARCHAR, \
                                         level VARCHAR, \
                                         song_id VARCHAR, \
                                         artist_id VARCHAR, \
                                         session_id INTEGER, \
                                         location VARCHAR, \
                                         user_agent VARCHAR);
 """)

users_table_create = ("""
    create table if not exists users (user_id VARCHAR PRIMARY KEY, \
                                     first_name VARCHAR, \
                                     last_name VARCHAR, \
                                     gender VARCHAR, \
                                     level VARCHAR);
""")

song_table_create = ("""
    create table if not exists song (song_id VARCHAR PRIMARY KEY, \
                                     title VARCHAR NOT NULL, \
                                     artist_id VARCHAR NOT NULL, \
                                     year VARCHAR NOT NULL);
""")

artist_table_create = ("""
    create table if not exists artist (artist_id VARCHAR PRIMARY KEY, \
                                       name VARCHAR NOT NULL, \
                                       location VARCHAR NOT NULL, \
                                       latitude FLOAT NOT NULL, \
                                       longitude FLOAT NOT NULL);
""")

time_table_create = ("""
    create table if not exists time (start_time BIGINT PRIMARY KEY, \
                                     hour INTEGER NOT NULL, \
                                     day INTEGER NOT NULL, \
                                     week INTEGER NOT NULL, \
                                     month INTEGER NOT NULL, \
                                     year INTEGER NOT NULL, \
                                     weekday INTEGER NOT NULL);
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {} 
    credentials 'aws_iam_role={}'
    json '{}' 
    compupdate off
    region 'us-west-2'
    timeformat 'epochmillisecs';
""").format(
    config.get("S3", "LOG_DATA"), 
    config.get("IAM_ROLE", "ARN"), 
    config.get("S3", "LOG_JSONPATH"))

staging_songs_copy = ("""
    copy staging_songs from {}
    credentials 'aws_iam_role={}'
    json 'auto' truncatecolumns
    BLANKSASNULL
    compupdate off
    region 'us-west-2'
    ACCEPTINVCHARS;
""").format(
    config['S3']['SONG_DATA'], 
    config['IAM_ROLE']['ARN'])
  
               
# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplay (songplay_id, start_time, user_id, level, \
                          song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT TIMESTAMP 'epoch' + (events.ts / 1000) * INTERVAL '1 second' as start_time, \
           staging_events.ts AS start_time, \
           staging_events.userid AS user_id, \
           staging_events.level, \
           staging_songs.song_id, \
           staging_songs.artist_id, \
           staging_events.sessionid AS session_id, \
           staging_events.location, \
           staging_events.userAgent as user_agent
    FROM staging_events
    JOIN staging_songs ON (staging_events.song = staging_songs.title)
    JOIN staging_songs ON (staging_events.artist = staging_songs.artist_name)
    JOIN staging_songs ON (staging_events.length = staging_songs.duration)
    WHERE staging_events.page = 'NextSong';
""")
    
users_table_insert = ("""
    INSERT INTO users (first_name, last_name, gender, level)
    SELECT DISTINCT
            staging_events.firstName AS first_name, \
            staging_events.lastName AS last_name, \
            staging_events.gender, \
            staging_events.level
    FROM staging_events 
    WHERE staging_events.page = 'NextSong'
    WHERE userid NOT IN (SELECT DISTINCT userid FROM users);                    
""")

song_table_insert = ("""
    INSERT INTO song (song_id, title, artist_id, year, duration)
    SELECT DISTINCT
        staging_songs.song_id, \
        staging_songs.title, \
        staging_songs.artist_id, \
        staging_songs.year, \
        staging_songs.duration
    FROM staging_songs;   
""")
    
artist_table_insert = ("""
    INSERT INTO artist (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT
        staging_songs.artist_id, \
        staging_songs.artist_name AS name, \
        staging_songs.artist_location AS location, \
        staging_songs.artist_latitude AS latitude
        staging_songs.artist_longitude AS longitude
    FROM staging_songs;
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT staging_events.ts, \
           start_time, \
           EXTRACT(hour FROM start_time) AS hour, \
           EXTRACT(day FROM start_time) AS day, \
           EXTRACT(week FROM start_time) AS week, \
           EXTRACT(month FROM start)
    FROM   'epoch'::date + (events.ts / 1000) * interval '1 second' AS start_time
    FROM staging_events;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, users_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, users_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, users_table_insert, song_table_insert, artist_table_insert, time_table_insert]
