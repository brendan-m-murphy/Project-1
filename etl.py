import io
import os
import glob
import psycopg2
import pandas as pd
import sql_queries
from staging import Stager

song_cols = {'artist_id': 'TEXT',
             'artist_name': 'TEXT',
             'artist_location': 'TEXT',
             'artist_latitude': 'DECIMAL',
             'artist_longitude': 'DECIMAL',
             'song_id': 'TEXT',
             'title': 'TEXT',
             'year': 'INTEGER',
             'duration': 'DECIMAL'}


def transform_song(file):
    df = pd.read_json(file, lines=True)
    cols = song_cols.keys()
    return df[cols].to_csv(sep='\t', header=False, index=False, na_rep='')


song_stager = Stager('data/song_data', 'song_staging', song_cols, transform_song)


log_cols = {'song': 'TEXT',
            'artist': 'TEXT',
            'userId': 'INTEGER',
            'firstName': 'TEXT',
            'lastName': 'TEXT',
            'gender': 'TEXT',
            'level': 'TEXT',
            'sessionId': 'TEXT',
            'location': 'TEXT',
            'userAgent': 'TEXT',
            'ts': 'TIMESTAMP',
            'hour': 'INT',
            'day': 'INT',
            'week': 'INT',
            'month': 'INT',
            'year': 'INT',
            'weekday': 'BOOLEAN'}


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
    df_time.columns = ['ts', 'hour', 'day', 'week', 'month', 'year', 'weekday']

    cols = ['song', 'artist', 'userId', 'firstName', 'lastName',
            'gender', 'level', 'sessionId', 'location', 'userAgent']

    return pd.concat([df[cols], df_time], axis=1).to_csv(sep='\t', header=False, index=False, na_rep='')


log_stager = Stager('data/log_data', 'log_staging', log_cols, transform_log)


def main():
    dsn = "host=127.0.0.1 dbname=sparkifydb user=student password=student"
    conn = psycopg2.connect(dsn)
    cur = conn.cursor()

    stagers = [song_stager, log_stager]
    
    for s in stagers:
        print('* Creating table', s.get_table_name())
        s.create_table(cur)
        conn.commit()
        
    try:
        for s in stagers:
            print('* Copying to table', s.get_table_name())
            s.copy(cur)
            conn.commit()
        for query in sql_queries.insert_queries:
            print('* Inserting into table', query.lstrip()[len('INSERT INTO '):].partition(' ')[0])
            cur.execute(query)
            conn.commit()
        for query in sql_queries.fk_queries:
            print('* Adding foreign key to table', query.lstrip()[len('ALTER SONGS '):].partition('\n')[0])
            cur.execute(query)
            conn.commit()
        for query in sql_queries.idx_queries:
            print('* Adding index to table', query.lstrip()[len('CREATE INDEX IF NOT EXISTS '):].partition(' ')[0])
            cur.execute(query)
            conn.commit()

        cur.execute("SELECT COUNT(*) FROM songplays WHERE artist_id IS NOT NULL;")
        print(cur.fetchone())
    finally:
        for s in stagers:
            print('* Dropping table', s.get_table_name())
            s.drop_table(cur)
            conn.commit()
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
