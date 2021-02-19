import pandas as pd

# Read tweet data
tweets = pd.read_csv(
    'data/test_set_tweets.txt',
    sep="\t",
    header=None,
    error_bad_lines=False,
    names=['user_id', 'tweet_id', 'tweet', 'timestamp']
)
tweets['timestamp'] = tweets['timestamp'].apply(lambda x: str(x).replace(' ', 'T'))

# Read user data
users = pd.read_csv(
    'data/test_set_users_utf8.txt',
    sep="\s*UT:\s|,",
    engine='python',
    header=None,
    names=['user_id', 'latitude', 'longitude']
)

# Join on 'user_id'
user_tweets = pd.merge(users, tweets, on="user_id")

# Export to csv
user_tweets.to_csv('data/tweet_data.csv', index=False, sep='\t')