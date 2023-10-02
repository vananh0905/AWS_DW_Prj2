import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
     Load staging tables into DB. This is used to copy data from one table to another
     
     @param cur - DB API cursor to use
     @param conn - DB API connection to use ( must be connected
    """
    # Execute all the queries in copy_table_queries.
    for query in copy_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
     Insert tables into the database. This is a wrapper around : func : ` insert_table_queries ` to allow us to do a series of queries at once without having to re - execute each query in the order they were added.
     
     @param cur - A cursor to be used for the query.
     @param conn - A DB API 2. 0 connection to be used
    """
    # Executes the insert_table_queries in the database.
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()