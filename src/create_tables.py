import env
import psycopg2
import sql_queries


def init_database():

    """
    Initializes the ``sparkifydb`` database
    """

    with psycopg2.connect(env.studentdb_dsn) as conn:
        conn.set_session(autocommit=True)
        with conn.cursor() as cur:
            # Create the 'sparkifydb' database
            cur.execute(sql_queries.sparkifydb_database_drop)
            cur.execute(sql_queries.sparkifydb_database_create)

    with psycopg2.connect(env.sparkifydb_dsn) as conn:
        conn.set_session(autocommit=True)
        with conn.cursor() as cur:
            # Create the 'songplays' table
            cur.execute(sql_queries.songplays_table_drop)
            cur.execute(sql_queries.songplays_table_create)
            # Create the 'users' table
            cur.execute(sql_queries.users_table_drop)
            cur.execute(sql_queries.users_table_create)
            # Create the 'songs' table
            cur.execute(sql_queries.songs_table_drop)
            cur.execute(sql_queries.songs_table_create)
            # Create the 'artists' table
            cur.execute(sql_queries.artists_table_drop)
            cur.execute(sql_queries.artists_table_create)
            # Create the 'time' table
            cur.execute(sql_queries.time_table_drop)
            cur.execute(sql_queries.time_table_create)


if __name__ == "__main__":
    init_database()
