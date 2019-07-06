# --------------------- #
# Database 'sparkifydb' #
# --------------------- #

sparkifydb_database_drop = 'DROP DATABASE IF EXISTS sparkifydb;'

sparkifydb_database_create = '''
    CREATE DATABASE sparkifydb
    WITH ENCODING 'utf8'
    TEMPLATE template0;
'''

# ----------------- #
# Table 'songplays' #
# ----------------- #

songplays_table_drop = 'DROP TABLE IF EXISTS songplays;'

songplays_table_create = '''
    CREATE TABLE songplays (
        songplay_id SERIAL PRIMARY KEY,
        start_time TIMESTAMP NOT NULL,
        user_id BIGINT NOT NULL,
        level TEXT NOT NULL,
        song_id TEXT NOT NULL,
        artist_id TEXT NOT NULL,
        session_id INT NOT NULL,
        location TEXT NOT NULL,
        user_agent TEXT NOT NULL
    );
'''

songplays_table_insert = '''
    INSERT INTO songplays(
        start_time,
        user_id,
        level,
        song_id,
        artist_id,
        session_id,
        location,
        user_agent
    )
    SELECT
        %s,
        %s,
        %s,
        songs.song_id,
        artists.artist_id,
        %s,
        %s,
        %s
    FROM songs
    LEFT OUTER JOIN artists ON songs.artist_id = artists.artist_id
    WHERE LOWER(songs.title) = LOWER(%s)
    AND LOWER(artists.name) = LOWER(%s)
'''

# ------------- #
# Table 'users' #
# ------------- #

users_table_drop = 'DROP TABLE IF EXISTS users;'

users_table_create = '''
    CREATE TABLE users (
        user_id BIGINT PRIMARY KEY,
        first_name TEXT NOT NULL,
        last_name TEXT NOT NULL,
        gender TEXT NOT NULL,
        level TEXT NOT NULL
    );
'''

users_table_bulk_insert = '''
    INSERT INTO users (
        user_id,
        first_name,
        last_name,
        gender,
        level
    )
    VALUES %s
    ON CONFLICT (user_id) DO UPDATE SET
        first_name = EXCLUDED.first_name,
        last_name = EXCLUDED.last_name,
        gender = EXCLUDED.gender,
        level = EXCLUDED.level;
'''

# ------------- #
# Table 'songs' #
# ------------- #

songs_table_drop = 'DROP TABLE IF EXISTS songs;'

songs_table_create = '''
    CREATE TABLE songs (
        song_id TEXT PRIMARY KEY,
        title TEXT NOT NULL,
        artist_id TEXT NOT NULL,
        year SMALLINT DEFAULT NULL,
        duration NUMERIC DEFAULT NULL
    );
'''

songs_table_insert = '''
    INSERT INTO songs (
        song_id,
        title,
        artist_id,
        year,
        duration
    )
    VALUES (
        %s,
        %s,
        %s,
        %s,
        %s
    )
    ON CONFLICT (song_id) DO UPDATE SET
        title = EXCLUDED.title,
        artist_id = EXCLUDED.artist_id,
        year = EXCLUDED.year,
        duration = EXCLUDED.duration;
'''

# --------------- #
# Table 'artists' #
# --------------- #

artists_table_drop = 'DROP TABLE IF EXISTS artists;'

artists_table_create = '''
    CREATE TABLE artists (
        artist_id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        location TEXT DEFAULT NULL,
        latitude NUMERIC DEFAULT NULL,
        longitude NUMERIC DEFAULT NULL
    );
'''

artists_table_insert = '''
    INSERT INTO artists (
        artist_id,
        name,
        location,
        latitude,
        longitude
    )
    VALUES (
        %s,
        %s,
        %s,
        %s,
        %s
    )
    ON CONFLICT (artist_id) DO UPDATE SET
        name = EXCLUDED.name,
        location = EXCLUDED.location,
        latitude = EXCLUDED.latitude,
        longitude = EXCLUDED.longitude;
'''

# ------------ #
# Table 'time' #
# ------------ #

time_table_drop = 'DROP TABLE IF EXISTS time;'

time_table_create = '''
    CREATE TABLE time (
        start_time TIMESTAMP PRIMARY KEY,
        hour SMALLINT NOT NULL,
        day SMALLINT NOT NULL,
        week SMALLINT NOT NULL,
        month SMALLINT NOT NULL,
        year SMALLINT NOT NULL,
        weekday SMALLINT NULL
    );
'''

time_table_bulk_insert = '''
    INSERT INTO time (
        start_time,
        hour,
        day,
        week,
        month,
        year,
        weekday
    )
    VALUES %s
    ON CONFLICT (start_time) DO NOTHING;
'''
