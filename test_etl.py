from unittest import TestCase, main
from etl import transform_song, transform_log_new
import io
import pandas as pd
import psycopg2


class TransformSong(TestCase):
    def test_transform_song(self):
        filepath = 'data/song_data/A/A/A/TRAAAAW128F429D538.json'
        f = transform_song(filepath)
        print(f)


class TransformLog(TestCase):
    def test_transform_log_new(self):
        filepath = 'data/log_data/2018/11/2018-11-01-events.json'
        f = transform_log_new(filepath)
        print(f)


if __name__ == '__main__':
    main()
