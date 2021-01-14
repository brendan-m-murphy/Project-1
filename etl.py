import os
import glob
import numpy as np
import psycopg2
from psycopg2 import Error
import pandas as pd
from sql_queries import *



def process_song_file(cur, filepath):
    df = pd.read_json(filepath, lines=True)
    
    artist_cols = ['artist_id', 'artist_name',
                    'artist_location', 'artist_latitude',
                       'artist_longitude']
    song_cols = ['song_id', 'title', 'artist_id', 'year', 'duration']

    # insert artist record
    artist_data = df.loc[:, artist_cols].values[0].tolist()
    cur.execute(artist_table_insert, tuple(artist_data))

    # insert song record
    song_data = df.loc[:, song_cols].values[0].tolist()
    cur.execute(song_table_insert, tuple(song_data))


# Helper functions to process log files
def ts_to_time_data(x):
    weekday = x.weekday() not in [5, 6]
    return (x, x.hour, x.day, x.week, x.month, x.year, weekday)

user_cols = ['userId', 'firstName', 'lastName', 'gender', 'level']
songplay_cols = ['userId', 'level',
                 'sessionId', 'location', 'userAgent']

def songplay_format(row, songid, artistid):
    ts = pd.to_datetime(row.ts, unit='ms')
    temp = row[songplay_cols].tolist()
    return [ts] + temp[:2] + [songid, artistid] + temp[2:]


def process_log_file(cur, filepath):
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    filt = df.loc[:, 'page'] == 'NextSong'
    df_next_song = df[filt]

    # convert timestamp column to datetime
    t0 = df_next_song.loc[:, 'ts']
    t = pd.to_datetime(t0, unit='ms')

    # insert time data records
    for i, x in t.iteritems():
        tup = ts_to_time_data(x)
        cur.execute(time_table_insert, tup)

    # load user table
    user_df0 = df.loc[:, user_cols]
    filt = user_df0.loc[:, 'userId'] != ''
    user_df = user_df0[filt]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df[filt].iterrows():

        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = tuple(songplay_format(row, songid, artistid))
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    dsn = "host=127.0.0.1 dbname=sparkifydb user=student password=student"
    conn = psycopg2.connect(dsn)
    cur = conn.cursor()

    try:
        process_data(cur, conn, filepath='data/song_data', func=process_song_file)
        process_data(cur, conn, filepath='data/log_data', func=process_log_file)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
