import os
import glob
import configparser
import psycopg2
import pandas as pd
from sql_queries import *

# CONFIG
config = configparser.ConfigParser()
config.read('env.cfg')

DB_HOST = config['DB']['DB_HOST']
DB_NAME = config['DB']['DB_NAME']
DB_USER = config['DB']['DB_USER']
DB_PASSWORD = config['DB']['DB_PASSWORD']


def process_song_file(cur, filepath):
    """
    (psycopg2.connect().cursor(), string: file path )Process the ETL of Song and Artist Data
    """
    # open song file
    df = pd.read_json(filepath,lines=True)
    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']]
    for i, row in song_data.iterrows():
        cur.execute(song_table_insert, list(row))
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_latitude', 'artist_longitude']]
    for i, row in artist_data.iterrows():
        cur.execute(artist_table_insert, list(row))


def process_log_file(cur, filepath):
    """
    (psycopg2.connect().cursor(), string: file path )Process the ETL of Time, User and Songplay Data
    """
    # open log file
    df = pd.read_json(filepath, typ='frame', lines=True)


    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'])
    
    # insert time data records
    time_data = ([t, t.dt.hour, t.dt.day, t.dt.weekofyear, t.dt.month, t.dt.year, t.dt.weekday])
    column_labels = ('timestamp', 'hour', 'day', 'week of year', 'month', 'year', 'weekday')
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, list(row))

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = ([pd.to_datetime(row.ts), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent])
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    (psycopg2.connect().cursor(), psycopg2.connect(), string: file path, function: process_song_file/process_log_file )Get all the files path under the provided directory then call the function parsed in the argument to Process each file
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
    conn = psycopg2.connect(
        "host={} dbname={} user={} password={}"\
            .format(DB_HOST, DB_NAME, DB_USER, DB_PASSWORD)
        )
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
