import io
import os
import glob
import psycopg2
import pandas as pd
import sql_queries
import staging
from tqdm import tqdm


def process_log_data(cur, conn):
    cur.execute("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT CAST(lstg.userId AS INT), lstg.firstName, lstg.lastName, lstg.gender, lstg.level
    FROM log_staging as lstg
    ON CONFLICT DO NOTHING;
    """)
    cur.execute("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT lstg.ts, lstg.hour, lstg.day, lstg.week, lstg.month, lstg.year, lstg.weekday
    FROM log_staging as lstg
    ON CONFLICT DO NOTHING;
    """)
    cur.execute("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT l.ts, CAST(l.userId AS INT), l.level, s.song_id, s.artist_id, CAST(l.sessionId AS INTEGER), l.location, l.userAgent
    FROM log_staging as l
    LEFT OUTER JOIN song_staging as s
    ON l.song = s.title AND l.artist = s.artist_name
    ON CONFLICT DO NOTHING;
    """)
    conn.commit()
    


def process_song_data(cur, conn):
    cur.execute("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT sstg.artist_id, sstg.artist_name, sstg.artist_location, sstg.artist_latitude, sstg.artist_longitude
    FROM song_staging as sstg
    ON CONFLICT DO NOTHING;
    """)
    cur.execute("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT sstg.song_id, sstg.title, sstg.artist_id, sstg.year, sstg.duration
    FROM song_staging as sstg
    ON CONFLICT DO NOTHING;
    """)
    conn.commit()


def create_fk_idx():
    pass


def main():
    dsn = "host=127.0.0.1 dbname=sparkifydb user=student password=student"
    conn = psycopg2.connect(dsn)
    cur = conn.cursor()

    staging.set_up(cur, conn)
    try:
        staging.etl(cur, conn)
        process_song_data(cur, conn)
        process_log_data(cur, conn)
    finally:
        staging.tear_down(cur, conn)
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
