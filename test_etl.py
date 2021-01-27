from unittest import TestCase, main
from etl import transform_song_df
from etl import copy_from_df
from etl import transform_log_df_time,  transform_log_df_user
import io
import pandas as pd
import psycopg2


def make_song_data():
    data = pd.DataFrame({'num_songs': [1],
                         'artist_id': ['artistid123'],
                         'artist_latitude': [12.34567],
                         'artist_longitude': [98.76543],
                         'artist_location': ['Buffalo, NY, USA'],
                         'artist_name': ['The Biebles'],
                         'song_id': ['songid123'],
                         'title': ["Ain't no Biebshine"],
                         'duration': [123.4],
                         'year': [2021]})

    artist  = pd.DataFrame({'artist_id': ['artistid123'],
                            'artist_name': ['The Biebles'],
                            'artist_location': ['Buffalo, NY, USA'],
                            'artist_latitude': [12.34567],
                            'artist_longitude': [98.76543]})

    song = pd.DataFrame({'song_id': ['songid123'],
                         'title': ["Ain't no Biebshine"],
                         'artist_id': ['artistid123'],
                         'year': [2021],
                         'duration': [123.4]})
    return data, artist, song



class TransformSongDfTestCase(TestCase):
    def test_artist(self):
        data, artist, song = make_song_data()
        artist_x, song_x = transform_song_df(data)
        self.assertEqual(artist.iloc[0, :].all(), artist_x.iloc[0, :].all())

    def test_song(self):
        data, artist, song = make_song_data()
        artist_x, song_x = transform_song_df(data)
        self.assertEqual(song.iloc[0, :].all(), song_x.iloc[0, :].all())


def make_log_data():
    log_raw = {"artist": ["Frumpies"],
               "auth": ["Logged In"],
               "firstName": ["Anabelle"],
               "gender": ["F"],
               "itemInSession": [0],
               "lastName": ["Simpson"],
               "length": [134.47791],
               "level": ["free"],
               "location": ["Philadelphia-Camden-Wilmington, PA-NJ-DE-MD"],
               "method": ["PUT"],
               "page": ["NextSong"],
               "registration": [1541044398796.0],
               "sessionId": [455],
               "song": ["Fuck Kitty"],
               "status": [200],
               "ts": [1541903636796],
               "userAgent": ["Mozilla 5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit 537.3"],
               "userId": ["69"]}

    log_users  = {"userId": ["69"],
                  "firstName": ["Anabelle"],
                  "lastName": ["Simpson"],
                  "gender": ["F"],
                  "level": ["free"]}

    log_time = {'start_time': [pd.Timestamp('2018-11-11 02:33:56.796')],
                'hour': [2],
                'day': [11],
                'week': [45],
                'month': [11],
                'year': [2018],
                'weekday': [False]}

    log_songplays = {'start_time': [pd.Timestamp('2018-11-11 02:33:56.796')],
                     'userId': ['69'],
                     'level': ['free'],
                     'song_id': ['songid123'],
                     'artist_id': ['artistid123'],
                     'sessionId': [455],
                     "location": ["Philadelphia-Camden-Wilmington, PA-NJ-DE-MD"],
                     "userAgent": ["Mozilla 5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit 537.3"]}
                  

    df = pd.DataFrame(log_raw)
    df_time = pd.DataFrame(log_time)
    df_users = pd.DataFrame(log_users)
    df_songplays = pd.DataFrame(log_songplays)

    return df, df_time, df_users, df_songplays


class TransformLogsTestCase(TestCase):
    def test_make_log_data(self):
        df, expected, _, _ = make_log_data()
        self.assertNotEqual(df.iloc[0, :].tolist(), expected.iloc[0, :].tolist())
    
    def test_transform_log_df_time(self):
        df, expected, _, _ = make_log_data()
        found = transform_log_df_time(df)
        self.assertEqual(expected.iloc[0, :].tolist(), found.iloc[0, :].tolist())

    def test_transform_log_df_user(self):
        df, _, expected, _ = make_log_data()
        found = transform_log_df_user(df)
        self.assertEqual(expected.iloc[0, :].tolist(), found.iloc[0, :].tolist())


class CopyFromDfTestCase(TestCase):
    def test_copy_from_df(self):
        df = pd.DataFrame({'id': [1, 2], 'name': ['Phil', 'Bob']})
        expected = [(1, 'Phil'), (2, 'Bob')]

        with psycopg2.connect('dbname=brendan') as conn:
            conn.set_session(autocommit=True)
            with conn.cursor() as cur:
                cur.execute('CREATE TABLE IF NOT EXISTS copy_test (id int, name text);')
                try:
                    copy_from_df(df, cur, 'copy_test')  # function to test
                    cur.execute('SELECT * FROM copy_test;')
                    found = cur.fetchall()
                finally:
                    cur.execute('DROP TABLE copy_test;')
        self.assertEqual(expected, found)


    def test_copy_from_df_with_index(self):
        df = pd.DataFrame({'num': [34, 31], 'name': ['Phil', 'Bob']})
        expected = [(1, 34, 'Phil'), (2, 31, 'Bob')]

        with psycopg2.connect('dbname=brendan') as conn:
            conn.set_session(autocommit=True)
            with conn.cursor() as cur:
                cur.execute('CREATE TABLE IF NOT EXISTS copy_test (id serial, num int, name text);')
                try:
                    copy_from_df(df, cur, 'copy_test', columns=('num', 'name'))  # function to test
                    cur.execute('SELECT * FROM copy_test;')
                    found = cur.fetchall()
                finally:
                    cur.execute('DROP TABLE copy_test;')
        self.assertEqual(expected, found)


if __name__ == '__main__':
    main()
