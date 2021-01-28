from unittest import TestCase, main
import logging
import pandas as pd
import psycopg2
from staging import extract, copy_from_df
from staging import transform_song, transform_log
from staging import create_staging_table, drop_staging_table


# We extract some .json files from 'data/song_data' and 'data/log_data'


# these files should contain one or more .json objects separated by newlines

# our song_data .json objects should have names:
# 'num_songs', 'artist_id', 'artist_latitude', 'artist_longitude',
# 'artist_location', 'artist_name', 'song_id', 'title', 'duration', 'year'
        
# our log_data .json objects should have names:
# 'artist', 'auth', 'firstName', 'gender', 'itemInSession',
# 'lastName', 'level', 'location', 'method', 'page', 'registration',
# 'sessionId', 'song', 'status', 'ts', 'userAgent', 'userID'


# We want to extract these .json files into DataFrames


# We create temporary tables for song_data and log_data


# We COPY data from our .json files into the temporary tables

class CopyTest(TestCase):
    def setUp(self):
        self.conn = psycopg2.connect("dbname=brendan")
        self.conn.autocommit = True
        self.cur = self.conn.cursor()


    def tearDown(self):
        self.cur.close()
        self.conn.close()

    
    def test_song_copy(self):
        create_staging_table('song_staging', self.cur)

        columns = ('artist_id', 'artist_name', 'artist_location',
                   'artist_latitude', 'artist_longitude', 'song_id',
                   'title', 'year', 'duration')

        try:
            files = extract('data/song_data')
            f = next(files)
            df = transform_song(f)
            copy_from_df(df, self.cur, 'song_staging', columns)
        except:
            logging.exception('Copying failed!')
        else:
            self.cur.execute("SELECT * FROM song_staging;")
            print(self.cur.fetchone())
        finally:
            drop_staging_table('song_staging', self.cur)


    def test_log_copy(self):
        create_staging_table('log_staging', self.cur)

        columns = ('userId', 'firstName', 'lastName',
                   'gender', 'level', 'sessionId',
                   'location', 'userAgent', 'ts',
                   'hour', 'day', 'week', 'month',
                   'year', 'weekday')
        
        try:
            files = extract('data/log_data')
            f = next(files)
            df = transform_log(f)
            copy_from_df(df, self.cur, 'log_staging', columns)
        except:
            logging.exception('ETL failed!')
        else:
            self.cur.execute("SELECT * FROM log_staging;")
            print(self.cur.fetchone())
        finally:
            drop_staging_table('log_staging', self.cur)


if __name__ == '__main__':
    main()
