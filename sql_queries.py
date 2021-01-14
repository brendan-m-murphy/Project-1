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

songplay_table_insert = ("""
                         INSERT INTO songplays
                             (start_time, user_id,
                              level, song_id,
                              artist_id, session_id,
                              location, user_agent)
                         VALUES (%s, %s, %s, %s,
                                 %s, %s, %s, %s);
                         """)

user_table_insert = ("""
                     INSERT INTO users
                         (user_id,
                          first_name, last_name,
                          gender, level)
                     VALUES (%s, %s, %s, %s, %s)
                     ON CONFLICT DO NOTHING;
                     """)

song_table_insert = ("""
                     INSERT INTO songs
                         (song_id,
                          title, artist_id,
                          year, duration)
                     VALUES (%s, %s, %s, %s, %s);
                     """)

artist_table_insert = ("""
                       INSERT INTO artists
                           (artist_id,
                            name, location,
                            latitude, longitude)
                       VALUES (%s, %s, %s, %s, %s)
                       ON CONFLICT DO NOTHING;
                       """)


time_table_insert = ("""
                     INSERT INTO time
                         (start_time, hour,
                          day, week,
                          month, year, weekday)
                     VALUES (%s, %s, %s, %s, %s, %s, %s)
                     ON CONFLICT DO NOTHING;
                     """)

# FIND SONGS

song_select = ("""
               SELECT s.song_id, a.artist_id
               FROM songs AS s
               JOIN artists AS a ON s.artist_id = a.artist_id
               WHERE s.title = %s AND a.name = %s AND s.duration = %s;
               """)

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create,
                        song_table_create, artist_table_create,
                        time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop,
                      song_table_drop, artist_table_drop,
                      time_table_drop]
constraint_queries = [set_fk1, set_fk2, set_fk3, set_fk4, set_fk5, set_idx1, set_idx2, set_idx3, set_idx4, set_idx5]
