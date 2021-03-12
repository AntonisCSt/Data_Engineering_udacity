import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

#A global variable for the process_log_file function. It will check if any none value was inserted within the songplays table
none_exists = False


def process_song_file(cur, filepath):
    """This function is responsible for the song and artist tables. It will open the song files in the pathfiel
    and insert both tables from the json files
    
    Arguments:
        cur: DB cursor connection
        filepath: path to files
    Return: insert into song and artist tables  
    
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = (df[['song_id','title','artist_id','year','duration']].values).tolist()[0]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = (df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']].values).tolist()[0]

    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """This function is responsible for tables related to song loggings. It will open the song files in the pathfile
    and insert both tables from the json files
    
     Arguments:
        cur: DB cursor connection
        filepath: path to files
    Return: insert into time, user songplays tables  
    
    """
    global none_exists
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    df['ts'] = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = (df['ts'].dt.time.values,df['ts'].dt.hour.values,df['ts'].dt.day.values,df['ts'].dt.weekofyear.values,df['ts'].dt.month.values,df['ts'].dt.year.values ,df['ts'].dt.dayofweek.values)
    column_labels = ('timestamp', 'hour', 'day', 'week of year', 'month', 'year', 'weekday')
    
    #creating a dictionary with time_data and column_labes to create the time_df dataframe
    time_data_dict = dict(zip(column_labels,time_data))
    time_df = pd.DataFrame(time_data_dict)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender','level']].copy()

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)
        
    #change daytime back to timestamp for the songplays table
    df['ts'] = df['ts'].dt.time
    
    # insert songplay records

    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None
            none_exists = True
        
   
        # insert songplay record
        songplay_data = (row['ts'],row['userId'],row['level'],songid,artistid, row['sessionId'],row['location'],row['userAgent'])
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """This function is responsible for processing the data. It will look at all the files from the directory, iterate through them and print out how many they have been processed
    
    Arguments:
        cur: DB cursor connection
        conn: connection with the dabase
        filepath: path to files
        func: function either the process_song_file or process_log_file
    Return: commints of table inserts from the functions  
    
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """main function of the etl.py. It conencts to the sparkifydb database and uses the process_data function to create and insert the tables. It also prints wether specific
    tables contain none values
    """
    global none_exists
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data/A', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data/2018', func=process_log_file)
    
    print("FINISHED!")
    if none_exists == False:
        print("songid and artistid values do not contain None values :D ")
    else:
        print("songid and artistid values contain None values D: ")
        
    conn.close()


if __name__ == "__main__":
    main()