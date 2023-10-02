import configparser


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
time_table_drop = "DROP TABLE IF EXISTS times;"

# CREATE TABLES
#staging tables: Will contain data from log_data, song_data
staging_events_table_create= (
"""
    CREATE TABLE staging_events (
    event_id INT IDENTITY(0,1) PRIMARY KEY,
    artist VARCHAR,
    auth VARCHAR,
    first_name VARCHAR,
    gender VARCHAR,
    item_in_session INT,
    last_name VARCHAR,
    length FLOAT,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration BIGINT,
    session_id BIGINT,
    song VARCHAR,
    status INT,
    ts BIGINT,
    user_agent VARCHAR,
    user_id VARCHAR
    );
""")
staging_songs_table_create = (
"""
    CREATE TABLE  IF NOT EXISTS staging_songs (
    staging_song_id INT IDENTITY(0,1) PRIMARY KEY,
    num_songs BIGINT,
    artist_id VARCHAR,
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location VARCHAR,
    artist_name VARCHAR,
    song_id VARCHAR,
    title VARCHAR,
    duration FLOAT,
    year INT
);     
""")
#---------------------------------------
#Final tables: staging tables will load into these tables one again
# 1.songplays
songplay_table_create = (
"""
        CREATE TABLE songplays (
        songplay_id INT IDENTITY(0,1) PRIMARY KEY,
        start_time BIGINT  REFERENCES times(start_time),
        user_id VARCHAR REFERENCES users(user_id),
        level VARCHAR ,
        song_id VARCHAR REFERENCES songs(song_id),
        artist_id VARCHAR REFERENCES artists(artist_id),
        session_id BIGINT ,
        location VARCHAR ,
        user_agent VARCHAR 
);  
""")
# 2.users
user_table_create = (
"""
    CREATE TABLE users (
        user_id VARCHAR PRIMARY KEY,
        first_name VARCHAR ,
        last_name VARCHAR ,
        gender VARCHAR ,
        level VARCHAR 
    );
""")
# 3. songs
song_table_create = (
"""
    CREATE TABLE songs (
        song_id VARCHAR PRIMARY KEY,
        title VARCHAR ,
        artist_id VARCHAR ,
        year integer,
        duration FLOAT
    );
""")
# 4. artists
artist_table_create = (
"""
    CREATE TABLE artists (
        artist_id VARCHAR PRIMARY KEY,
        name VARCHAR,
        location VARCHAR ,
        lattitude FLOAT ,
        longitude FLOAT 
    );
""")
# 5. times
time_table_create = (
"""
    CREATE TABLE times (
        start_time TIMESTAMP PRIMARY KEY NOT NULL SORTKEY,
        hour integer ,
        day integer ,
		week integer ,
        weekday VARCHAR,
        month integer,
        year integer 
    );
""")

#LOAD DATA
# Stating tabbe: load data from json data files
staging_events_copy = (
"""
    copy staging_events from '{}'
    credentials 'aws_iam_role={}'
    json '{}'
    region 'us-west-2';
""").format(config['S3']['LOG_DATA'],
            config['IAM_ROLE']['IAM_ROLE_ARN'],
            config['S3']['LOG_JSONPATH'])

staging_songs_copy = (
"""
    copy staging_songs from '{}'
    credentials 'aws_iam_role={}'
    json 'auto'
    region 'us-west-2';
""").format(config['S3']['SONG_DATA'],
            config['IAM_ROLE']['IAM_ROLE_ARN'])

# Final table: load data from staging tables
user_table_insert = (
"""
    INSERT INTO users(user_id, first_name, last_name, gender, level)
    SELECT DISTINCT
        user_id, 
        first_name, 
        last_name, 
        gender, 
        level
    FROM staging_events
    WHERE page='NextSong';
""")

song_table_insert = (
"""
    INSERT INTO songs(song_id, title, artist_id, year, duration)
    SELECT DISTINCT
        song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_songs;
""")

artist_table_insert = (
"""
    INSERT INTO artists(artist_id, name, location, lattitude, longitude)
    SELECT DISTINCT
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM staging_songs;
""")

time_table_insert = (
"""
    INSERT INTO times(start_time, hour, day, week, weekday, month, year)
    SELECT DISTINCT
        timestamp 'epoch' + se.ts/1000 * interval '1 second' AS start_time,
        EXTRACT(hour from start_time) AS hour,
        EXTRACT(day from start_time) AS day,
        EXTRACT(week from start_time) AS week,
        EXTRACT(dow from start_time) AS weekday,
        EXTRACT(month from start_time) AS month,
        EXTRACT(year from start_time) AS year
    FROM staging_events as se
    WHERE se.page='NextSong';
""")

songplay_table_insert = (
"""
    INSERT INTO songplays(start_time, user_id, level, song_id,
                            artist_id, session_id, location, user_agent)
    SELECT DISTINCT
        e.ts as start_time,
        e.user_id,
        e.level,
        s.song_id,
        s.artist_id,
        e.session_id,
        e.location,
        e.user_agent
    FROM staging_events e
    LEFT JOIN staging_songs s ON e.song = s.title
                AND e.artist = s.artist_name
                AND e.length = s.duration
	WHERE e.page='NextSong';
""")

#QUERY LISTS
drop_table_queries = [staging_events_table_drop, 
                      staging_songs_table_drop, 
                      songplay_table_drop, 
                      user_table_drop, 
                      song_table_drop, 
                      artist_table_drop, 
                      time_table_drop]

create_table_queries = [staging_events_table_create, 
                        staging_songs_table_create, 
                        user_table_create, 
                        song_table_create, 
                        artist_table_create, 
                        time_table_create,
                        songplay_table_create]

copy_table_queries = [staging_events_copy, 
                      staging_songs_copy]

insert_table_queries = [user_table_insert, 
                       song_table_insert, 
                       artist_table_insert, 
                       time_table_insert,
                       songplay_table_insert]
