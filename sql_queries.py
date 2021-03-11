# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS sparkifydb.songplays"
user_table_drop = "DROP TABLE IF EXISTS sparkifydb.users"
song_table_drop = "DROP TABLE IF EXISTS sparkifydb.songs"
artist_table_drop = "DROP TABLE IF EXISTS sparkifydb.artists"
time_table_drop = "DROP TABLE IF EXISTS sparkifydb.time"

# CREATE TABLES

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS
songplays (start_time time, user_id int, level varchar, song_id varchar NOT NULL, artist_id varchar NOT NULL, session_id int, location varchar, user_agent text, PRIMARY KEY (song_id, artist_id));""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS
users (user_id int PRIMARY KEY, first_name varchar, last_name varchar, gender varchar, level varchar);""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS
songs (song_id varchar PRIMARY KEY, title text, artist_id varchar , year int, duration float);""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS
artists (artist_id varchar PRIMARY KEY, name varchar, location varchar, latitude float, longitude float);""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS
time (start_time time PRIMARY KEY, hour int, day int, week int, month int, year int, weekday int);""")

# INSERT RECORDS

songplay_table_insert = ("""INSERT INTO songplays( start_time, user_id , level , song_id , artist_id, session_id , location, user_agent )   VALUES ( %s, %s, %s, %s,%s,%s,%s,%s) ON CONFLICT (song_id,artist_id) 
DO NOTHING
""")

user_table_insert = ("""INSERT INTO users(user_id , first_name , last_name , gender , level ) 
                 VALUES (%s, %s, %s, %s, %s) ON CONFLICT (user_id) 
DO NOTHING
""")

song_table_insert = (""" INSERT INTO songs(song_id, title, artist_id , year , duration ) 
                 VALUES (%s,%s,%s,%s, %s) ON CONFLICT (song_id) 
DO NOTHING""")

artist_table_insert = (""" INSERT INTO artists(artist_id, name, location, latitude, longitude ) 
                 VALUES (%s, %s, %s, %s, %s) ON CONFLICT (artist_id) DO NOTHING""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day , week , month , year , weekday)   VALUES (%s, %s, %s, %s, %s,%s,%s) ON CONFLICT (start_time) 
DO NOTHING""")

# FIND SONGS

song_select = (""" SELECT songs.song_id, artists.artist_id 
                   FROM songs
                   INNER JOIN artists ON songs.artist_id = artists.artist_id
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]