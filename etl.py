import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

"""
This module contains functions for extracting, transforming, and loading
(ETL) music data from S3 into a Redshift data warehouse.
"""
def load_staging_tables(cur, conn):
    """
        Copies data from S3 buckets into staging tables in Redshift.
        Args:
            cur: The database cursor.
            conn: The database connection.
        """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
        Inserts data from staging tables into the analytics tables.

        Args:
            cur: The database cursor.
            conn: The database connection.
        """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
       Main entry point for the ETL process.

       Establishes a connection to the Redshift cluster, loads data into staging
       tables, inserts transformed data into the analytics tables, and then
       closes the connection.
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