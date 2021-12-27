import json
import time
import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import execute_batch
from search_tweets_v2 import search_tweets_v2
from store_in_psql import insert_data_to_psql
from clean_text import clean_text
from transformers import pipeline, AutoModelForSequenceClassification, BertJapaneseTokenizer, BertTokenizer
from datetime import datetime, date, timedelta

# connecting to Postgres database
DATABASE_NAME = "public_opinion_db"
TWEETS_TABLE_NAME = "tweets_processed"
USER_TABLE_NAME = "users"
DB_USER = "mmiyazaki"
params = "dbname=%s user=%s" % (DATABASE_NAME, DB_USER)

model = AutoModelForSequenceClassification.from_pretrained('daigo/bert-base-japanese-sentiment') 
tokenizer = BertJapaneseTokenizer.from_pretrained('cl-tohoku/bert-base-japanese-whole-word-masking')
nlp = pipeline("sentiment-analysis", model=model, tokenizer=tokenizer, )

# parameters
with open('politicians.txt', 'r') as f:
    list_politicians = f.readlines()

today = date.today().strftime("%Y-%m-%d")
yesterday = (datetime.strptime(today, '%Y-%m-%d') + timedelta(days=-1)).strftime("%Y-%m-%d")

def main():

    for politician in list_politicians:

        print('scraping ', politician.strip(), 'for ', yesterday)

        # search tweets and users
        original_tweets_list, user_list = search_tweets_v2(
            politician=politician.strip(),
            results_per_call=10,
            end_time=yesterday,
            max_results=5,
            max_pages=1,
        )

        # prepare tweets and users
        original_tweets_list_columns = list(original_tweets_list[0].keys())
        user_list_columns = list(user_list[0].keys())

        tweet_data = [
            tuple(tweet[column] for column in original_tweets_list_columns)
            for tweet in original_tweets_list
        ]
        user_data = [
            tuple(user[column] for column in user_list_columns) for user in user_list
        ]

        # append cleaned tweets
        tweet_data = [
            i + (clean_text(i[3]),) for i in tweet_data
        ]
        
        # append sentiment analysis
        tweet_data = [
            i + tuple(nlp(tweet_data[0][-1])[0].values()) for i in tweet_data
        ]

        columns = original_tweets_list_columns + ["tweets_cleaned", "sentiment", "score"]

        # insert tweets into tweet table
        # TO DO : drop and create a new table with three additional columns
        # tweets_cleaned, sentiment_label, sentiment_score
        """
        insert_data_to_psql(
            data=tweet_data,
            params=params,
            table_name=TWEETS_TABLE_NAME,
            columns=columns,
        )
        """
        conn = None
        try:
            # connect to the PostgreSQL database
            conn = psycopg2.connect(params)
            # create a new cursor
            cur = conn.cursor()
            full_query = "INSERT INTO tweets_processed VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)ON CONFLICT ON CONSTRAINT tweets_processed_pkey DO NOTHING"
            execute_batch(cur, full_query, tweet_data)
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

        # insert users into users table
        """
        insert_data_to_psql(
            data=user_data,
            params=params,
            table_name=USER_TABLE_NAME,
            columns=user_list_columns,
        )
        """
        conn = None
        try:
            # connect to the PostgreSQL database
            conn = psycopg2.connect(params)
            # create a new cursor
            cur = conn.cursor()
            full_query = "INSERT INTO users VALUES(%s, %s, %s)ON CONFLICT ON CONSTRAINT users_pkey DO NOTHING"
            execute_batch(cur, full_query, user_data)
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


if __name__ == "__main__":
    main()