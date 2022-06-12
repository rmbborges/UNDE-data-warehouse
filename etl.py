import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    ETL Step I: Load the S3 bucket files into Redshift for the staging tables creation.
       
    Parameters
        - cur (psycopg.Cursor): Postgres cursor.
        - conn (psycopg2.Connection): Postgres database connnection
    
    Returns
        None
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    ETL Step II: Process data from staging tables and insert data into data warehouse tables.
       
    Parameters
        - cur (psycopg.Cursor): Postgres cursor.
        - conn (psycopg2.Connection): Postgres database connnection
    
    Returns
        None
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Execute both load_staging_tables and insert_tables function
       
    Parameters
        None
    
    Returns
        None
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()