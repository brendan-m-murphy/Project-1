import io
import os
import glob
import psycopg2
from psycopg2 import sql
import pandas as pd
import sql_queries


def extract(filepath):
    """
    Yields all .json files from filepath.
    """
    for root, _, _ in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            yield f



class Stager:
    """
    A class that writes .json files to a temporary PostgreSQL table.

    The most important attribute of a Stager is its transformer, which
    must be carefully designed to match the input files to the staging table.
    
    The rest of the class encapsulates creating, copying, and dropping
    the staging table.

    WARNING: this class is not safe to use with untrusted parameters,
    due to use of string formatting to create and drop tables.

    It is also possible to over-write a table using the create_table() method.
    Make sure that table_name does not already exist.

    Attributes
    ----------

    filepath: string or path-like object

    table_name: string, name of staging table

    columns: dict or list, names of columns in staging table, with data types
    as values, if passed a dictionary.

    transformer: function, takes a filepath to a .json file and returns
    a CSV string with \t separator and null values as a null string.

    """
    def __init__(self, filepath, table_name, columns, transformer):
        self.filepath = filepath
        self.table_name = table_name
        if isinstance(columns, dict):
            self.columns = columns
        elif isinstance(columns, list):
            self.columns = {k: 'TEXT' for k in columns}
        else:
            raise ValueError('columns must be a dict or list')
        self.transformer = transformer

    def get_table_name(self):
        return self.table_name

    def set_columns(self, cols):
        self.columns = cols

    def get_columns(self, dtypes=False):
        """
        Returns column names as a list.

        If dtypes=True, returns column names and
        their data types as a dict.
        """
        if dtypes:
            return self.columns
        else:
            return self.columns.keys()

    def copy(self, cur):
        with io.StringIO() as f:
            for file in extract(self.filepath):
                f.write(self.transformer(file))
            f.seek(0)
            cur.copy_from(f, self.table_name, columns=tuple(self.get_columns()), null='')

    def create_table(self, cur):
        cols_str = ', '.join([k + ' ' + v for k, v in self.columns.items()])
        query = 'CREATE UNLOGGED TABLE {} (id SERIAL, {});'.format(self.table_name, cols_str)
        cur.execute(query)

    def drop_table(self, cur):
        query = 'DROP TABLE {};'.format(self.table_name)
        cur.execute(query)

    


def etl_song_staging(cur):
    """
    Populates the song_staging table via cursor cur.

    The song_staging table must be created before running this function.
    Commit must be run after running this function.

    Assumes that data/song_data contains  .json files with .json
    objects separated by newlines.
    
    Each .json object should contain the following names:
        
    'num_songs', 'artist_id', 'artist_latitude',
    'artist_longitude', 'artist_location', 'artist_name'
    'song_id', 'title', 'duration', 'year'

    """
    cols = ['artist_id', 'artist_name', 'artist_location',
            'artist_latitude', 'artist_longitude', 'song_id',
            'title', 'year', 'duration']

    def transform_song(file):
        df = pd.read_json(file, lines=True)
        return df[cols].to_csv(sep='\t', header=False, index=False, na_rep='')

    song_stager = Stager('data/song_data', 'song_staging', cols, transform_song)
    song_stager.copy(cur)

    # files = extract('data/song_data')
    # with io.StringIO() as csv:
    #     for f in files:
    #         df = transform_song(f)
    #         df.to_csv(csv, sep='\t', header=False, index=False, na_rep='')
    #     csv.seek(0)
    #     cur.copy_from(csv, 'song_staging', columns=tuple(cols), null='')



def etl_log_staging(cur):
    """
    Populates log_staging table via the cursor cur.

    Assumes that the filepath 'data/log_data'  points to
    .json file with .json objects separated by newlines.
    
    Each .json object should contain the following names:
            
    'artist', 'auth', 'firstName', 'gender',
    'itemInSession', 'lastName', 'level',
    'location', 'method', 'page', 'registration',
    'sessionId', 'song', 'status', 'ts', 'userAgent', 'userID'
  
    """
    cols = ['song', 'artist', 'userId', 'firstName', 'lastName',
            'gender', 'level', 'sessionId', 'location', 'userAgent']
    cols_time = ['ts', 'hour', 'day', 'week', 'month', 'year', 'weekday']

    def transform_time(x):
        weekday = x.weekday() not in [5, 6]
        return pd.Series([x, x.hour, x.day, x.week, x.month, x.year, weekday])
    
    def transform_log(file):
        df = pd.read_json(file, lines=True)
        filt = df['userId'] != ''
        df = df[filt]
        filt = df['page'] == 'NextSong'
        df = df[filt]

        t = pd.to_datetime(df.ts, unit='ms')
        df_time = t.apply(transform_time)
        df_time.columns = cols_time

        return pd.concat([df[cols], df_time], axis=1)

    files = extract('data/log_data')
    with io.StringIO() as csv:
        for f in files:
            df = transform_log(f)
            df.to_csv(csv, sep='\t', header=False, index=False, na_rep='')
        csv.seek(0)
        cur.copy_from(csv, 'log_staging', columns=tuple(cols + cols_time), null='')


class StringIteratorIO(io.TextIOBase):
    """
    File-like object.
    Needs to implement read() and readline().
    """
    pass


def set_up(cur, conn):
#    cur.execute(sql_queries.create_song_staging)
    song_cols = {'artist_id': 'TEXT',
                 'artist_name': 'TEXT',
                 'artist_location': 'TEXT',
                 'artist_latitude': 'DECIMAL',
                 'artist_longitude': 'DECIMAL',
                 'song_id': 'TEXT',
                 'title': 'TEXT',
                 'year': 'INTEGER',
                 'duration': 'DECIMAL'}
    song_stager = Stager('data/song_data', 'song_staging', song_cols, lambda: None)
    song_stager.create_table(cur, conn)
    cur.execute(sql_queries.create_log_staging)
    conn.commit()


def etl(cur, conn):
    etl_song_staging(cur)
    etl_log_staging(cur)
    conn.commit()


def tear_down(cur, conn):
    cur.execute("DROP TABLE song_staging;")
    cur.execute("DROP TABLE log_staging;")

