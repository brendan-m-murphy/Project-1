from cProfile import Profile
import csv
import datetime
import io
import json
import os
import glob
import time
import pandas as pd
from pstats import Stats
import psycopg2
import create_tables
import sql_queries
from staging import Stager


# defining columns and transformer for `song_stager`
song_cols = {'artist_id': 'TEXT', 'artist_name': 'TEXT',
             'artist_location': 'TEXT', 'artist_latitude': 'DECIMAL',
             'artist_longitude': 'DECIMAL', 'song_id': 'TEXT',
             'title': 'TEXT', 'year': 'INTEGER',
             'duration': 'DECIMAL'}


def transform_song(filepath):
    """
    Load a .json object from filepath and return it as
    a tab separated string ending with a newline.
    """
    f = json.load(open(filepath))
    return '\t'.join([str(v) if (v := f[k]) else ''
                      for k in song_cols.keys()]) + '\n'


song_stager = Stager('data/song_data', 'song_staging', song_cols, transform_song)


# defining columns and transformer for `log_stager`
log_cols = {'song': 'TEXT', 'artist': 'TEXT',
            'userId': 'INTEGER', 'firstName': 'TEXT',
            'lastName': 'TEXT', 'gender': 'TEXT',
            'level': 'TEXT', 'sessionId': 'TEXT',
            'location': 'TEXT', 'userAgent': 'TEXT',
            'ts': 'TIMESTAMP', 'hour': 'INT',
            'day': 'INT', 'week': 'INT',
            'month': 'INT', 'year': 'INT',
            'weekday': 'BOOLEAN'}


def transform_log(filepath):
    """
    Load .json objects (separated by newlines) from filepath, transform
    them, and return them as tab separated strings on separate lines.
    """
    cols = ['song', 'artist', 'userId', 'firstName', 'lastName',
            'gender', 'level', 'sessionId', 'location', 'userAgent']

    result = ''
    with open(filepath, 'rt') as f:
        for line in f:
            jf = json.loads(line)
            if jf['userId'] and (jf['page'] == 'NextSong'):
                jf['userAgent'] = jf['userAgent'].strip('"')
                
                temp1 = '\t'.join([str(v) if (v := jf[k]) else '' for k in cols])

                t = round(jf['ts']/1000)  # UNIX timestamp, ignore ms
                x = datetime.datetime.fromtimestamp(t)
                
                temp2 = '\t'.join([x.strftime("%Y-%m-%d %H:%M:%S"),
                                   str(x.hour),
                                   str(x.day),
                                   str(x.isocalendar()[1]),
                                   str(x.month),
                                   str(x.year),
                                   str(x.weekday() not in [5, 6])])

                result += temp1 + '\t' + temp2 + '\n'
    return result


log_stager = Stager('data/log_data', 'log_staging', log_cols, transform_log)


def main():
    """
    Run script creating and loading Postgres database from data directory.

    Steps:
    1. Run create_tables.py to create Postgress database 'sparkifydb' and
    star schema with tables: songplays, users, time, artists, songs.

    2. Connect to sparkifydb.

    3. Create staging tables song_staging and log_staging.

    4. Create profilers.

    5. In the body of `try`:
    - copy data to staging tables using profilers.
    - insert data into star schema
    - set foreign keys and indices
    - execute a simple query to check if at least one entry in
    `songplays` has a non-null artist_id

    6. In the body of `finally`: drop the staging tables and 
    close the connection.

    7. Print the profiling statistics, showing the top 10% of
    operations sorted by time.
    """

    create_tables.main()
    
    dsn = "host=127.0.0.1 dbname=sparkifydb user=student password=student"
    conn = psycopg2.connect(dsn)
    cur = conn.cursor()

    stagers = [song_stager, log_stager]
    
    for s in stagers:
        print('* Creating table', s.get_table_name())
        s.create_table(cur)
        conn.commit()
        

    profilers = [Profile(), Profile()]
    try:
        for p, s in zip(profilers, stagers):
            print('* Copying to table', s.get_table_name())
            p.runcall(lambda: s.copy(cur, stream=True))
            conn.commit()

        print('* Inserting into star schema')
        for query in sql_queries.insert_queries:
            cur.execute(query)
            conn.commit()
        print('* Setting foreign keys')
        for query in sql_queries.fk_queries:
            cur.execute(query)
            conn.commit()
        print('* Setting indices')
        for query in sql_queries.idx_queries:
            cur.execute(query)
            conn.commit()

        cur.execute("SELECT COUNT(*) FROM songplays WHERE artist_id IS NOT NULL;")
        print('* Number of songplays with artist_id not NULL:', cur.fetchone()[0])
    finally:
        for s in stagers:
            print('* Dropping table', s.get_table_name())
            s.drop_table(cur)
            conn.commit()
        cur.close()
        conn.close()
        
    for profiler in profilers:
        stats = Stats(profiler)
        stats.strip_dirs()
        stats.sort_stats('time')
        stats.print_stats(.1)

if __name__ == "__main__":
    main()
