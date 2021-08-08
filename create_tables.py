import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

def drop_tables(cur, conn):
    """
    This function parses through the drop tables query defined in the sql_queries.py script and drops tables
    """

    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

def create_tables(cur, conn):
     """
     This function parses through the create tables queries defined in the sql_queries.py script and creates tables
     """

    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    """
    This function executes the parameters for connecting to the redshift cluster as defined in the dwh.cfg script
    """

    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
