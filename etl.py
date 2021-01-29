import io
import os
import glob
import psycopg2
import pandas as pd
import sql_queries
import staging
from tqdm import tqdm


# columns for artist and song queries
artist_cols = ['artist_id', 'artist_name',
               'artist_location', 'artist_latitude',
               'artist_longitude']
song_cols = ['song_id', 'title', 'artist_id', 'year', 'duration']


def process_song_file(cur, filepath):
    """
    Populates a database via cur with the file located at filepath.

    Assumes that filepath points to .json file with .json
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
        
    Two tables are populated from this data:
        1. `songs`
        2. `artists`


    Parameters
    ----------
    cur : psycopg2 Cursor
        Cursor for connection to target database.
    filepath : string or path-like object
        Location of song .json file.

    Returns
    -------
    None.

    """
    df = pd.read_json(filepath, lines=True)

    # insert artist record
    artist_data = df.loc[:, artist_cols].values[0].tolist()
    cur.execute(sql_queries.artist_table_insert, tuple(artist_data))

    # insert song record
    song_data = df.loc[:, song_cols].values[0].tolist()
    cur.execute(sql_queries.song_table_insert, tuple(song_data))


# columns for user and songplay queries
user_cols = ['userId', 'firstName', 'lastName', 'gender', 'level']


def ts_to_time_data(x):
    weekday = x.weekday() not in [5, 6]
    return (x, x.hour, x.day, x.week, x.month, x.year, weekday)


# columns for songplays table
songplay_cols = ['userId', 'level',
                 'sessionId', 'location', 'userAgent']


def songplay_format(row, songid, artistid):
    """
    Helper function for `process_log_files`.

    Takes an instance (row) and songid, artistid and returns a list
    of parameters to execute the `songplay_table_insert` query.
    
    The parameters returned are:
        converted timestamp (from row),
        userId and level (from row),
        songid and artistid,
        location and userAgent (from row).


    Parameters
    ----------
    row : pandas Series
        Row from DataFrame containing extracted log files.
    songid, artistid : string
        Ids from songs and artists tables, found by `song_select` query.

    Returns
    -------
    list
        Parameters to pass to `songplay_table_insert` query.

    """
    ts = pd.to_datetime(row.ts, unit='ms')
    temp = row[songplay_cols].tolist()
    return [ts] + temp[:2] + [songid, artistid] + temp[2:]


def process_log_file(cur, filepath):
    """
    Populates a database via cur with the file located at filepath.

    Assumes that filepath points to .json file with .json
    objects separated by newlines.
    
    Each .json object should contain the following names:
            
        - `artist`
        - `auth`
        - `firstName`
        - `gender`
        - `itemInSession`
        - `lastName`
        - `level`
        - `location`
        - `method`
        - `page`
        - `registration`
        - `sessionId`
        - `song`
        - `status`
        - `ts`
        - `userAgent`
        - `userID`
        
    Three tables are populated from this data:
        1. `time`
        2. `users`
        3. `songplays`

    Parameters
    ----------
    cur : psycopg2 Cursor
        Cursor for connection to target database.
    filepath : string or path-like object
        Location of log .json file.

    Returns
    -------
    None.

    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong page
    filt = df.loc[:, 'page'] == 'NextSong'
    df_next_song = df[filt]

    # convert timestamp column to datetime
    t0 = df_next_song.loc[:, 'ts']
    t = pd.to_datetime(t0, unit='ms')

    # insert time data records
    for i, x in t.iteritems():
        params = ts_to_time_data(x)
        cur.execute(sql_queries.time_table_insert, params)

    # filter out rows with missing userId
    filt = df_next_song.loc[:, 'userId'] != ''
    df_next_song_userId = df_next_song[filt]

    # insert user records
    for i, row in df_next_song_userId[user_cols].iterrows():
        cur.execute(sql_queries.user_table_insert, row)

    # insert songplay records
    for index, row in df_next_song_userId.iterrows():

        # get songid and artistid from song and artist tables
        cur.execute(sql_queries.song_select,
                    (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = tuple(songplay_format(row, songid, artistid))
        cur.execute(sql_queries.songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Retrieves all .json files from dir `filepath`, transforms them
    according to `func`, and loads them to the database connected to
    `conn` via the cursor `cur`.

    Parameters
    ----------
    cur : psycopg2 Cursor
        Cursor for the connection `conn`.
    conn : psycopg2 Connection
        Connection to target database.
    filepath : string or path-like object
        Directory containing .json files to load.
    func : function
        Transformation function. Either `process_song_file`, or
        `process_log_file`.

    Returns
    -------
    None.

    """
    # get all files matching extension from directory
    all_files = []
    for root, _, _ in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process, with progress bar
    progress_bar = tqdm(enumerate(all_files, 1), total=num_files,
                        unit='files',
                        desc="Processing '{}'".format(filepath))
    for i, datafile in progress_bar:
        func(cur, datafile)
        conn.commit()


def populate_artist_table(cur):
    cur.execute("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT sstg.artist_id, sstg.artist_name, sstg.artist_location, sstg.artist_latitude, sstg.artist_longitude
    FROM song_staging as sstg
    ON CONFLICT DO NOTHING;
    """)
    

def populate_song_table(cur):
    cur.execute("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT sstg.song_id, sstg.title, sstg.artist_id, sstg.year, sstg.duration
    FROM song_staging as sstg
    ON CONFLICT DO NOTHING;
    """)


def process_song_data(cur, conn):
    staging.create_staging_table('song_staging', cur)
    columns = ('artist_id', 'artist_name', 'artist_location',
               'artist_latitude', 'artist_longitude', 'song_id',
               'title', 'year', 'duration')

    try:
        staging.etl_song_staging(cur)
        populate_artist_table(cur)
        populate_song_table(cur)
        conn.commit()
    finally:
        staging.drop_staging_table('song_staging', cur)


def main():
    dsn = "host=127.0.0.1 dbname=sparkifydb user=student password=student"
    conn = psycopg2.connect(dsn)
    cur = conn.cursor()

    try:
        process_song_data(cur, conn)
        process_data(cur, conn,
                     filepath='data/log_data', func=process_log_file)
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
