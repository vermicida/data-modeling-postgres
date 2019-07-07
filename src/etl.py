import env
import glob
import math
import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import sql_queries


# -------------------- #
# Song data processing #
# -------------------- #


def normalize_empty_data(row):

    """
    Normalizes the empty data in the given row.

    Args:
        row (pandas.Series): The row containing the song data.

    Returns:
        (pandas.Series): A normalized version of the row.
    """

    if row['year'] == 0:
        row['year'] = None

    if row['artist_location'] is not None and row['artist_location'] == '':
        row['artist_location'] = None

    if row['artist_latitude'] is None or math.isnan(row['artist_latitude']):
        row['artist_latitude'] = None

    if row['artist_longitude'] is None or math.isnan(row['artist_longitude']):
        row['artist_longitude'] = None

    return row


def insert_songs(df, cursor):

    """
    Inserts ``songs`` into the database.

    Args:
        df (pandas.DataFrame): The table containing the songs info.
        cursor (Cursor): The current PostgreSQL connection cursor.
    """

    columns = [
        'song_id',
        'title',
        'artist_id',
        'year',
        'duration'
    ]
    cursor.execute(
        sql_queries.songs_table_insert,
        df[columns].values[0].tolist()
    )


def insert_artists(df, cursor):

    """
    Inserts ``artists`` into the database.

    Args:
        df (pandas.DataFrame): The table containing the artists info.
        cursor (Cursor): The current PostgreSQL connection cursor.
    """

    columns = [
        'artist_id',
        'artist_name',
        'artist_location',
        'artist_latitude',
        'artist_longitude'
    ]
    cursor.execute(
        sql_queries.artists_table_insert,
        df[columns].values[0].tolist()
    )


def process_song_file(cursor, path):

    """
    Processes a JSON file containing song info.

    Args:
        cursor (Cursor): The current PostgreSQL connection cursor.
        path (str): The path to the JSON file.
    """

    # Open the given JSON file path.
    df = pd.read_json(path, lines=True)
    df = df.apply(normalize_empty_data, axis=1)

    # Insert songs and artists in the database.
    insert_songs(df, cursor)
    insert_artists(df, cursor)


# ------------------- #
# Log data processing #
# ------------------- #


def expand_time_info(row):

    """
    Uses the timestamp within the given row to expands the time info.

    Args:
        row (pandas.Series): The row containing the log data.

    Returns:
        (pandas.Series): A more detailed version of the row.
    """

    ts = pd.to_datetime(row['ts'], unit='ms')

    row['ts'] = ts
    row['ts_hour'] = ts.hour
    row['ts_day'] = ts.day
    row['ts_week'] = ts.week
    row['ts_month'] = ts.month
    row['ts_year'] = ts.year
    row['ts_weekday'] = ts.weekday()

    return row


def insert_times(df, cursor):

    """
    Inserts ``times`` into the database.

    Args:
        df (pandas.DataFrame): The table containing the times info.
        cursor (Cursor): The current PostgreSQL connection cursor.
    """

    columns = [
        'ts',
        'ts_hour',
        'ts_day',
        'ts_week',
        'ts_month',
        'ts_year',
        'ts_weekday'
    ]
    execute_values(
        cursor,
        sql_queries.time_table_bulk_insert,
        df[columns].drop_duplicates(subset='ts').values.tolist()
    )


def insert_users(df, cursor):

    """
    Inserts ``users`` into the database.

    Args:
        df (pandas.DataFrame): The table containing the users info.
        cursor (Cursor): The current PostgreSQL connection cursor.
    """

    columns = [
        'userId',
        'firstName',
        'lastName',
        'gender',
        'level'
    ]
    execute_values(
        cursor,
        sql_queries.users_table_bulk_insert,
        df[columns].drop_duplicates(subset='userId').values.tolist()
    )


def insert_songplays(df, cursor):

    """
    Inserts ``songplays`` into the database.

    Args:
        df (pandas.DataFrame): The table containing the songplays info.
        cursor (Cursor): The current PostgreSQL connection cursor.
    """

    columns = [
        'ts',
        'userId',
        'level',
        'sessionId',
        'location',
        'userAgent',
        'song',
        'artist',
        'length'
    ]
    for i, row in df[columns].iterrows():
        cursor.execute(
            sql_queries.songplays_table_insert_select,
            row.tolist()
        )


def process_log_file(cursor, path):

    """
    Processes a JSON file containing log info.

    Args:
        cursor (Cursor): The current PostgreSQL connection cursor.
        path (str): The path to the JSON file.
    """

    # Open the given JSON file path.
    df = pd.read_json(path, lines=True)
    df = df.query('page == \'NextSong\'')
    df = df.apply(expand_time_info, axis=1)

    # Insert times, users and songplays in the database.
    insert_times(df, cursor)
    insert_users(df, cursor)
    insert_songplays(df, cursor)


# ------ #
# Common #
# ------ #


def find_json(root):

    """
    Searches for the JSON files within the given directory.

    Args:
        root (str): The directory from which to start the search.

    Yields:
        str: The absolute path of the next JSON file.
    """

    for path in glob.glob(os.path.join(root, '**', '*.json'), recursive=True):
        yield os.path.abspath(path)


def main():

    """
    Processes some JSON files containing info related songs, artists and logs.
    """

    with psycopg2.connect(env.sparkifydb_dsn) as conn:
        conn.set_session(autocommit=True)
        with conn.cursor() as cur:

            for path in find_json('data/song_data'):
                process_song_file(cur, path)

            for path in find_json('data/log_data'):
                process_log_file(cur, path)


if __name__ == '__main__':
    main()
