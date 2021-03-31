import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


# CONFIG
config = configparser.ConfigParser()
config.read('env.cfg')

DB_HOST = config['DB']['DB_HOST']
DB_NAME = config['DB']['DB_NAME']
DB_NAME_DEFAULT = config['DB']['DB_NAME_DEFAULT']
DB_USER = config['DB']['DB_USER']
DB_PASSWORD = config['DB']['DB_PASSWORD']
DB_PORT = config['DB']['DB_PORT']

def create_database():
    """
    - Creates and connects to the sparkifydb
    - Returns the connection and cursor to sparkifydb
    """
    
    # connect to default database
    conn = psycopg2.connect(
        "host={} dbname={} user={} password={}"\
            .format(DB_HOST, DB_NAME_DEFAULT, DB_USER, DB_PASSWORD, DB_PORT)
            )
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS {}".format(DB_NAME))
    cur.execute("CREATE DATABASE {} WITH ENCODING 'utf8' TEMPLATE template0".format(DB_NAME))

    # close connection to default database
    conn.close()    
    
    # connect to sparkify database
    conn = psycopg2.connect(
        "host={} dbname={} user={} password={}"\
           .format(DB_HOST, DB_NAME, DB_USER, DB_PASSWORD) 
        )
    cur = conn.cursor()
    
    return cur, conn


def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries` list. 
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Drops (if exists) and Creates the sparkify database. 
    
    - Establishes connection with the sparkify database and gets
    cursor to it.  
    
    - Drops all the tables.  
    
    - Creates all tables needed. 
    
    - Finally, closes the connection. 
    """
    cur, conn = create_database()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
