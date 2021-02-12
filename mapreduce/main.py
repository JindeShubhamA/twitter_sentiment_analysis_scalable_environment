from random import random
from time import sleep
import pyspark


# use dictionaries as our mock databases
tweets_db = {}
users_db = {}

# tweet_index = 0
# user_index = 0
tweets_file_name = '../dataset/test_set_tweets.txt'
users_file_name = '../dataset/test_set_users.txt'
# opening a ~600mb file is actually fine according to this stackoverflow link:
# https://stackoverflow.com/questions/28538388/python-open-function-memory-usage/28538436
tweets_file = open(tweets_file_name)
users_file = open(users_file_name)


# for ease of implementation I'm going to assume that the raw tweet info will already be split
def handle_tweet(user_id, tweet_id, tweet_body, tweet_time):
    print("tweet:", user_id, tweet_id, tweet_body, tweet_time)
    if tweet_id in tweets_db.get(user_id, {}):
        print(f"Tweet with id {tweet_id} is already saved")

    new_entry = {"tweet_body": tweet_body, "tweet_time": tweet_time}

    # create userid entry in tweets_db if necessary
    if not user_id in tweets_db:
        tweets_db[user_id] = {}

    tweets_db[user_id][tweet_id] = new_entry
    return



# again for ease of implementation I'm going to assume the raw information is already split
def handle_user(user_id, latt, long):
    print("user:", user_id, latt, long)
    if user_id in users_db:
        print(f"User with id {user_id} is already saved")

    new_entry = {"lattitude": latt, "longitude": long}

    users_db[user_id] = new_entry
    return


def simulate_next_tweet():
    tweet_parts = tweets_file.readline().split('\t')
    if len(tweet_parts) != 4:
        return

    handle_tweet(tweet_parts[0], tweet_parts[1], tweet_parts[2], tweet_parts[3])
    return


def simulate_next_user():
    user_parts = users_file.readline().split('\t')
    coord_parts = user_parts[1].split(',')
    handle_user(user_parts[0], coord_parts[0][4:], coord_parts[1])
    return


# simulates new tweets and new users coming in at random intervals
def do_stuff():
    while True:
        if random() < 0.5:
            simulate_next_tweet()
        else:
            simulate_next_user()
        sleep(random()*5)

    return


if __name__ == '__main__':
    do_stuff()

tweets_file.close()
users_file.close()