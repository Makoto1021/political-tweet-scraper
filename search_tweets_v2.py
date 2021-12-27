from searchtweets import (
    ResultStream,
    gen_request_parameters,
    load_credentials,
    collect_results,
)

import json
import time
import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import execute_batch

search_args = load_credentials(
    "credentials_v2.yaml", yaml_key="search_tweets_v2", env_overwrite=False
)


def search_tweets_v2(
    politician="高市早苗",
    results_per_call=20,
    end_time="2021-09-23",
    max_results=10,
    max_pages=1,
):

    # generate query
    query = gen_request_parameters(
        query=politician,
        results_per_call=results_per_call,
        end_time=end_time,
        # start_time=None,
        expansions="attachments.poll_ids,author_id,entities.mentions.username,geo.place_id,in_reply_to_user_id,referenced_tweets.id,referenced_tweets.id.author_id",
        # max_results=10, # max is 100
        place_fields="contained_within,country,country_code,full_name,geo,id,name,place_type",
        # since_id=None,
        tweet_fields="author_id,conversation_id,created_at,entities,geo,id,in_reply_to_user_id,lang,public_metrics,referenced_tweets,text",  # non_public_metrics, organic_metrics are not available
        granularity=None,
    )


    # start streaming
    rs = ResultStream(
        request_parameters=query,
        max_results=max_results,
        max_pages=max_pages,
        **search_args
    )
    tweet_data = list(rs.stream())

    # create original_tweet_table and user_table
    original_tweets_list = []
    i = 0
    for batch in tweet_data:
        # print("Batch", i)

        for tweet in batch["data"]:
            if "referenced_tweets" in tweet.keys():
                # exclude retweeted or quoted tweets
                if tweet["referenced_tweets"][0]["type"] in ("retweeted", "quoted"):
                    pass
            else:
                # it's the original tweet or in reply to somebody
                original_tweets_list.append(
                    {
                        "tweet_created_at": tweet["created_at"],
                        "tweet_id": tweet["id"],
                        "user_id": tweet["author_id"],
                        # "user_name": str(tweet.user.name),
                        # "followers_count": tweet.user.followers_count,
                        # "account_created_at": tweet.user.created_at,
                        "tweet": str(tweet["text"]),
                        "politician": politician,
                        "retweet_count": tweet["public_metrics"]["retweet_count"]
                        if "public_metrics" in tweet.keys()
                        else None,
                        "reply_count": tweet["public_metrics"]["reply_count"]
                        if "public_metrics" in tweet.keys()
                        else None,
                        "like_count": tweet["public_metrics"]["like_count"]
                        if "public_metrics" in tweet.keys()
                        else None,
                        "quote_count": tweet["public_metrics"]["quote_count"]
                        if "public_metrics" in tweet.keys()
                        else None,
                        # "retweeted_id": retweeted_id,
                        # "retweeted_user_id": retweeted_user_id,
                        "lang": tweet["lang"] if "lang" in tweet.keys() else None,
                        "entities": None,  # tweet["entities"] if "entities" in tweet.keys() else None,
                        "conversation_id": tweet["conversation_id"]
                        if "conversation_id" in tweet.keys()
                        else None,
                    }
                )
        i += 1

    user_list = []
    user_id_list = []

    i = 0
    for batch in tweet_data:
        for user in batch["includes"]["users"]:

            if user["id"] in user_id_list:
                pass
            else:
                user_id_list.append(user["id"])
                user_list.append(
                    {
                        "user_id": user["id"],
                        "name": user["name"],
                        "username": user["username"],
                    }
                )
        i += 1

    return original_tweets_list, user_list
