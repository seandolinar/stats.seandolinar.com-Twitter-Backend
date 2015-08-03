__author__ = 'seandolinar'

import time
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import tweepy as ty
from pymongo import MongoClient
import pymongo
from datetime import datetime
import json
import LIB_race_emoji as lb

import datetime_insert as dt
import timeline_interactions as tli

client = MongoClient('box-jenkins.local', 27017) #'192.168.1.100'
db = client['twitter']
sn = db['screen_name']
tl = db['diversity_test10']

# with open('twitter_credentials.json', 'r') as credential_file:
#
#     twitter_cred = json.load(credential_file)
#
#
# ckey = twitter_cred['ckey']
# consumer_secret = twitter_cred['consumer_secret']
# access_token_key = twitter_cred['access_token_key']
# access_token_secret = twitter_cred['access_token_secret']

handle_iter = sn.find({})
# handle_iter = sn.find({'_id': {'$in': ['penguins']}})

for handle in handle_iter:

    print handle
    since_id_list = []
    tweets = tl.find({'user.screen_name': handle['_id']})

    for tweet in tweets:

        since_id_list.append(tweet['_id'])

    try:
        sn.update({'_id': handle['_id']}, {'$set': { 'since_id':max(since_id_list)}})

    except ValueError:
        print '0 tweets'
        # print ValueError

    print '@' + handle['_id'] + ' updated!'