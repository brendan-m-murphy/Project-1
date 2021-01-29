import io
import os
import glob
import psycopg2
import pandas as pd


def extract(filepath):
    """
    Yields all .json files from filepath.
    """
    for root, _, _ in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            yield f


def transform_song(file):
    df = pd.read_json(file, lines=True)
    cols = ['artist_id', 'artist_name',
            'artist_location', 'artist_latitude',
            'artist_longitude', 'song_id', 'title',
            'year', 'duration']
    return df[cols]


def transform_log(file):
    df = pd.read_json(file, lines=True)
    filt = df['userId'] != ''
    df = df[filt]
    filt = df['page'] == 'NextSong'
    df = df[filt]

    cols = ['userId', 'firstName', 'lastName', 'gender',
            'level', 'sessionId', 'location', 'userAgent']

    t = pd.to_datetime(df.ts, unit='ms')
    def f(x):
        weekday = x.weekday() not in [5, 6]
        return pd.Series([x, x.hour, x.day, x.week, x.month, x.year, weekday])
    df_time = t.apply(f)
    df_time.columns = ['ts', 'hour', 'day', 'week', 'month', 'year', 'weekday']

    return pd.concat([df[cols], df_time], axis=1)


def copy_from_df(df, cur, table, columns=None):
    """
    Copy DataFrame `df` to PostgreSQL table 'table' via cursor `cur`.

    The number and type of columns in `df` must match the
    columns in `table`, unless `columns` is set.

    Parameters
    ----------
    df: DataFrame

    cur: psycopg2 cursor

    table: string, name of table in Database connected to cur that we wish
    to copy to.

    columns: tuple, names of columns in table that correspond to the columns of df
    """
    with io.StringIO() as f:
        df.to_csv(f, sep='\t', header=False, index=False, na_rep='')
        f.seek(0)
        cur.copy_from(f, table, columns=columns, null='')


def create_staging_table(name, cur):
    if name == 'song_staging':
        cur.execute("""
        DROP TABLE IF EXISTS song_staging;
        CREATE UNLOGGED TABLE song_staging (
        id SERIAL,
        artist_id TEXT,
        artist_name TEXT,
        artist_location TEXT,
        artist_latitude DECIMAL,
        artist_longitude DECIMAL,
        song_id TEXT,
        title TEXT,
        year INTEGER,
        duration DECIMAL
        );
        """)
    elif name == 'log_staging':
        cur.execute("""
        DROP TABLE IF EXISTS log_staging;
        CREATE UNLOGGED TABLE log_staging (
        id SERIAL,
        userId TEXT,
        firstName TEXT,
        lastName TEXT,
        gender TEXT,
        level TEXT,
        sessionId TEXT,
        location TEXT,
        userAgent TEXT,
        ts TIMESTAMP,
        hour INT,
        day INT,
        week INT,
        month INT,
        year INT,
        weekday BOOLEAN
        );
        """)
        pass
    else:
        raise ValueError("'name' must be 'song_staging' or 'log_staging', received:", name)


def drop_staging_table(name, cur):
    if name == 'song_staging':
        cur.execute("DROP TABLE song_staging;")
    elif name == 'log_staging':
        cur.execute("DROP TABLE log_staging;")
    else:
        raise ValueError("'name' must be 'song_staging' or 'log_staging', received:", name)


def etl_song_staging(cur):
    """
    Populates the song_staging table via cursor cur.

    The song_staging table must be created before running this function.
    Commit must be run after running this function.

    Assumes that data/song_data contains  .json files with .json
    objects separated by newlines.
    
    Each .json object should contain the following names:
        
        - `num_songs`
        - `artist_id`
        - `artist_latitude`
        - `artist_longitude`
        - `artist_location`
        - `artist_name`
        - `song_id`
        - `title`
        - `duration`
        - `year`
        
    
    Parameters
    ----------
    cur : psycopg2 Cursor
        Cursor for connection to target database.

    Returns
    -------
    None.

    """
    columns = ('artist_id', 'artist_name', 'artist_location',
               'artist_latitude', 'artist_longitude', 'song_id',
               'title', 'year', 'duration')


    files = extract('data/song_data')
    for f in files:
        df = transform_song(f)
        copy_from_df(df, cur, 'song_staging', columns)

