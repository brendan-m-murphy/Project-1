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
                         year integer,
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
                         weekday boolean
                         );
                     """)


# query to create foreign keys and indices for each fk
set_fk_idx = ("""
              ALTER TABLE songplays
              ADD CONSTRAINT FOREIGN KEY (start_time)
                  REFERENCES time
                  ON DELETE RESTRICT ON UPDATE CASCADE
              ADD CONSTRAINT FOREIGN KEY (user_id)
                  REFERENCES users
                  ON DELETE RESTRICT ON UPDATE CASCADE
              ADD CONSTRAINT FOREIGN KEY (song_id)
                  REFERENCES songs
                  ON DELETE RESTRICT ON UPDATE CASCADE
              ADD CONSTRAINT FOREIGN KEY (artist_id)
                  REFERENCES artists
                  ON DELETE RESTRICT ON UPDATE CASCADE
              ADD INDEX idx_start_time (start_time)
              ADD INDEX idx_user_id (user_id)
              ADD INDEX idx_song_id (song_id)
              ADD INDEX idx_artist_id (artist_id);
              """)


# query for foreign key and index in songs
set_fk_idx2 = ("""
               ALTER TABLE songs
               ADD CONSTRAINT FOREIGN KEY (artist_id)
                   REFERENCES artists
                   ON DELETE RESTRICT ON UPDATE CASCADE
               ADD INDEX idx_artist_id (artist_id);
               """)


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
                     VALUES (%s, %s, %s, %s, %s);
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
                       VALUES (%s, %s, %s, %s, %s);
                       """)


time_table_insert = ("""
                     INSERT INTO time
                         (start_time, hour,
                          day, week,
                          month, weekday)
                     VALUES (%s, %s, %s, %s, %s, %s);
                     """)

# FIND SONGS

song_select = ("""
               
               """)

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create,
                        song_table_create, artist_table_create,
                        time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop,
                      song_table_drop, artist_table_drop,
                      time_table_drop]
constraint_queries = [set_fk_idx, set_fk_idx2]
