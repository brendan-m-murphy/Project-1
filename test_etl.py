from unittest import TestCase, main
from etl import transform_song_df
from etl import df_to_csv, df_to_csv2, copy_from_df
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
    pass


class TransformLogDfTestCase(TestCase):
    pass


class DfToCsvTestCase(TestCase):
    def test_df_to_csv(self):
        csv_string = 'a\t1\nb\t2\nc\t3\n'
        with io.StringIO(csv_string) as f:
            df = pd.read_csv(f, sep='\t', header=None)
        csv = df_to_csv(df)
        self.assertEqual(csv_string, csv)

    def test_df_to_csv2(self):
        csv_string = 'a\t1\nb\t2\nc\t3\n'
        with io.StringIO(csv_string) as f:
            df = pd.read_csv(f, sep='\t', header=None)
        csv = df_to_csv2(df)
        self.assertEqual(csv_string, csv)


class CopyFromDfTestCase(TestCase):
    def test_copy_from_df(self):
        df = pd.DataFrame({'id': [1, 2], 'name': ['Phil', 'Bob']})
        expected = [(1, 'Phil'), (2, 'Bob')]

        with psycopg2.connect('dbname=brendan') as conn:
            conn.set_session(autocommit=True)
            with conn.cursor() as cur:
                cur.execute('CREATE TABLE IF NOT EXISTS copy_test (id int, name text);')
                try:
                    copy_from_df(df, cur, 'copy_test')
                    cur.execute('SELECT * FROM copy_test;')
                    found = cur.fetchall()
                finally:
                    cur.execute('DROP TABLE copy_test;')
        self.assertEqual(expected, found)


if __name__ == '__main__':
    main()
