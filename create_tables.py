import psycopg2
from sql_queries import create_table_queries, drop_table_queries
from sql_queries import constraint_queries


def create_database():
    """
    - Creates and connects to the sparkifydb
    - Returns the connection and cursor to sparkifydb
    """
    
    # connect to default database
    #  conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
    conn = psycopg2.connect("dbname=brendan")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")
    cur.execute("GRANT ALL ON DATABASE sparkifydb to student")

    # close connection to default database
    conn.close()    
    
    # connect to sparkify database
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    #  conn = psycopg2.connect("dbname=sparkifydb user=student password=student")
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


def set_constraints(cur, conn):
    """
    Creates foreign key constraints and sets indices
    """
    for query in constraint_queries:
        try:
            cur.execute(query)
        except psycopg2.Error as e:
            print("Error adding constraints:", e)
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
    set_constraints(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()