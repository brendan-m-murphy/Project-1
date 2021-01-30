# CREATE STAGING TABLES
create_song_staging = """
DROP TABLE IF EXISTS song_staging;
CREATE UNLOGGED TABLE song_staging (
id SERIAL,
artist_id TEXT,
artist_name TEXT,
artist_location TEXT,
artist_latitude DECIMAL,
artist_longitude DECIMAL,
song_id TEXT,
title TEXT,
year INTEGER,
duration DECIMAL);
"""

create_log_staging = """
DROP TABLE IF EXISTS log_staging;
CREATE UNLOGGED TABLE log_staging (
id SERIAL,
song TEXT,
artist TEXT,
userId TEXT,
firstName TEXT,
lastName TEXT,
gender TEXT,
level TEXT,
sessionId TEXT,
location TEXT,
userAgent TEXT,
ts TIMESTAMP,
hour INT,
day INT,
week INT,
month INT,
year INT,
weekday BOOLEAN);
"""


# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"


# CREATE TABLES

songplay_table_create = ("""
                         CREATE TABLE IF NOT EXISTS songplays (
                             songplay_id serial PRIMARY KEY,
                             start_time timestamp,
                             user_id integer,
                             level char(4),
                             song_id char(18),
                             artist_id char(18),
                             session_id integer,
                             location varchar,
                             user_agent varchar
                             );
                        """)

user_table_create = ("""
                     CREATE TABLE IF NOT EXISTS users (
                         user_id integer PRIMARY KEY,
                         first_name varchar,
                         last_name varchar,
                         gender varchar,
                         level char(4)
                         );
                     """)

song_table_create = ("""
                     CREATE TABLE IF NOT EXISTS songs (
                         song_id char(18) PRIMARY KEY,
                         title varchar,
                         artist_id char(18),
                         year smallint,
                         duration numeric(9, 5)
                         );
                     """)

artist_table_create = ("""
                       CREATE TABLE IF NOT EXISTS artists (
                           artist_id varchar(18) PRIMARY KEY,
                           name varchar,
                           location varchar,
                           latitude numeric(7, 5),
                           longitude numeric(8, 5)
                           );
                       """)

time_table_create = ("""
                     CREATE TABLE IF NOT EXISTS time (
                         start_time timestamp PRIMARY KEY,
                         hour smallint,
                         day smallint,
                         week smallint,
                         month smallint,
                         year smallint,
                         weekday boolean
                         );
                     """)

# query to create foreign keys and indices for each fk
set_fk1 = ("""
ALTER TABLE songplays
ADD CONSTRAINT fk__songplays__time
FOREIGN KEY (start_time) 
REFERENCES time
ON DELETE RESTRICT ON UPDATE CASCADE;""")


set_fk2 = ("""
ALTER TABLE songplays
ADD CONSTRAINT fk__songplays__users
FOREIGN KEY (user_id)
REFERENCES users
ON DELETE RESTRICT ON UPDATE CASCADE;""")


set_fk3 = ("""
ALTER TABLE songplays
ADD CONSTRAINT fk__songplays__songs
FOREIGN KEY (song_id)
REFERENCES songs
ON DELETE RESTRICT ON UPDATE CASCADE;""")


set_fk4 = ("""
ALTER TABLE songplays
ADD CONSTRAINT fk__songplays__artists
FOREIGN KEY (artist_id)
REFERENCES artists
ON DELETE RESTRICT ON UPDATE CASCADE;""")


set_fk5 = ("""
ALTER TABLE songs
ADD CONSTRAINT fk__songs__artists
FOREIGN KEY (artist_id)
REFERENCES artists
ON DELETE RESTRICT ON UPDATE CASCADE;""")


set_idx1 = "CREATE INDEX IF NOT EXISTS idx_start_time ON songplays (start_time);"
set_idx2 = "CREATE INDEX IF NOT EXISTS idx_user_id ON songplays (user_id);"
set_idx3 = "CREATE INDEX IF NOT EXISTS idx_song_id ON songplays (song_id);"
set_idx4 = "CREATE INDEX IF NOT EXISTS idx_artist_id ON songplays (artist_id);"
set_idx5 = "CREATE INDEX IF NOT EXISTS idx_artist_id ON songs (artist_id);"



# INSERT RECORDS

songplay_table_insert = """
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT l.ts, CAST(l.userId AS INT), l.level, s.song_id, s.artist_id, CAST(l.sessionId AS INTEGER), l.location, l.userAgent
FROM log_staging as l
LEFT OUTER JOIN song_staging as s
ON l.song = s.title AND l.artist = s.artist_name
ON CONFLICT DO NOTHING;
"""

user_table_insert = """
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT CAST(lstg.userId AS INT), lstg.firstName, lstg.lastName, lstg.gender, lstg.level
FROM log_staging as lstg
ON CONFLICT DO NOTHING;
"""

time_table_insert = """
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT lstg.ts, lstg.hour, lstg.day, lstg.week, lstg.month, lstg.year, lstg.weekday
FROM log_staging as lstg
ON CONFLICT DO NOTHING;
"""

song_table_insert = """
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT sstg.song_id, sstg.title, sstg.artist_id, sstg.year, sstg.duration
FROM song_staging as sstg
ON CONFLICT DO NOTHING;
"""

artist_table_insert = """
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT sstg.artist_id, sstg.artist_name, sstg.artist_location, sstg.artist_latitude, sstg.artist_longitude
FROM song_staging as sstg
ON CONFLICT DO NOTHING;
"""


# QUERY LISTS
create_table_queries = [songplay_table_create, user_table_create,
                        song_table_create, artist_table_create,
                        time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop,
                      song_table_drop, artist_table_drop,
                      time_table_drop]
insert_queries = [artist_table_insert, song_table_insert, user_table_insert, time_table_insert, songplay_table_insert]
fk_queries = [set_fk1, set_fk2, set_fk3, set_fk4, set_fk5]
idx_queries = [set_idx1, set_idx2, set_idx3, set_idx4, set_idx5]
