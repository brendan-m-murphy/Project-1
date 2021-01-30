from unittest import TestCase, main
from etl import transform_song
import io
import pandas as pd
import psycopg2


class TransformSong(TestCase):
    def test_transform_song(self):
        filepath = 'data/song_data/A/A/A/TRAAAAW128F429D538.json'
        f = transform_song(filepath)
        print(f)




if __name__ == '__main__':
    main()
