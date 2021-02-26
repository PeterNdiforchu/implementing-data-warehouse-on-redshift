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
                                               IteminSession INTEGER, \
                                               lastName VARCHAR, \
                                               length FLOAT NULL, \
                                               level VARCHAR, \
                                               location VARCHAR, \
                                               method VARCHAR, \
                                               page VARCHAR, \
                                               registration FLOAT, \
                                               session_id INTEGER, \
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
                                              artist_longitute FLOAT, \
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
    INSERT INTO songplay (songplay_id, start_time, user_id, level \
                          song_id, artist_id, session_id, location, user_agent)
    SELECT 
            se.ts AS start_time, \
            se.userid AS user_id, \
            se.level, \
            ss.song_id, \
            ss.artist_id, \
            se.sessionid AS session_id, \
            se.location, \
            se.userAgent as user_agent
    FROM staging_events se
    JOIN staging_songs ss ON (se.song = ss.title)
    JOIN staging_songs ss ON (se.artist = ss.artist_name)
    JOIN staging_songs ss ON (se.length = ss.duration)
    WHERE se.page = 'NextSong';
""")
    
users_table_insert = ("""
    INSERT INTO users (first_name, last_name, gender, level)
    SELECT DISTINCT
                    se.firstName AS first_name, \
                    se.lastName AS last_name, \
                    se.gender, \
                    se.level
    FROM staging_events se
    WHERE se.page = 'NextSong'
    WHERE userid NOT IN (SELECT DISTINCT userid FROM users);                    
""")

song_table_insert = ("""
    INSERT INTO song (song_id, title, artist_id, year, duration)
    SELECT DISTINCT
        ss.title, \
        ss.artist_id, \
        ss.year, \
        ss.duration
    FROM staging_songs ss;   
""")
    
artist_table_insert = ("""
    INSERT INTO artist (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT
        ss.artist_id, \
        ss.artist_name AS name, \
        ss.artist_location AS location, \
        ss.artist_latitude AS latitude
        ss.artist_longitude AS longitude
    FROM staging_songs ss;
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT ts, \
           start_time, \
           EXTRACT(hour FROM start_time) AS hour, \
           EXTRACT(day FROM start_time) AS day, \
           EXTRACT(week FROM start_time) AS week, \
           EXTRACT(month FROM start)
    FROM (T0_CHAR('1970-01-01'::date + ts/1000 * interval '1 second')::integer) AS start_time
    FROM staging_events;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, users_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, users_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, users_table_insert, song_table_insert, artist_table_insert, time_table_insert]
