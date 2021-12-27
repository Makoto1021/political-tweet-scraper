import json
import time
import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import execute_batch
from search_tweets_v2 import search_tweets_v2


def insert_data_to_psql(data, params, table_name, columns):
    base_statement = "INSERT INTO {}({}) ".format(
        table_name, "{}, " * (len(columns) - 1) + "{}"
    ).format(*columns)
    full_statement = (
        base_statement
        + "VALUES({}) ON CONFLICT ON CONSTRAINT {}_pkey DO NOTHING;".format(
            "%s, " * (len(columns) - 1) + "%s", table_name
        )
    )
    print("full_statement:", full_statement)

    conn = None
    try:
        # connect to the PostgreSQL database
        conn = psycopg2.connect(params)
        # create a new cursor
        cur = conn.cursor()

        # execute the INSERT statement
        # cur.executemany(full_statement, tweet_data) # this is slow.
        execute_batch(cur, full_statement, data)
        print("query executed")

        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    finally:
        if conn is not None:
            conn.close()

def execute_many(df, params, table_name, columns):
    """
    Using cursor.executemany() to insert the dataframe
    """
    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in df.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list(df.columns))
    # SQL quert to execute
    # query  = "INSERT INTO %s(%s) VALUES(%%s,%%s,%%s)" % (table_name, columns)
    base_statement = "INSERT INTO {}({}) ".format(
        table_name, "{}, " * (len(columns) - 1) + "{}"
    ).format(*columns)
    query = (
        base_statement
        + "VALUES({}) ON CONFLICT ON CONSTRAINT {}_pkey DO NOTHING;".format(
            "%s, " * (len(columns) - 1) + "%s", table_name
        )
    )
    conn = psycopg2.connect(params)
    cursor = conn.cursor()
    try:
        cursor.executemany(query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("execute_many() done")
    cursor.close()
