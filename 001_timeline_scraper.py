#-*- coding: utf-8 -*-
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
# from retrying import retry

import datetime_insert as dt
import timeline_interactions as tli
# from datetime_insert import datetime_mongoinsert


client = MongoClient('box-jenkins.local', 27017)
db = client['sd_twitter']
sn = db['screen_name']
tl = db['engagement']


# twitter OAuth
with open('twitter_credentials.json', 'r') as credential_file:
    twitter_cred = json.load(credential_file)


ckey = twitter_cred['ckey']
consumer_secret = twitter_cred['consumer_secret']
access_token_key = twitter_cred['access_token_key']
access_token_secret = twitter_cred['access_token_secret']

auth = ty.OAuthHandler(ckey, consumer_secret)
auth.set_access_token(access_token_key, access_token_secret)


API = ty.API(auth, timeout=180)

#gets screen names to be scraped
date = '2015-07-19'

handle_iter = sn.find({'lg': {'$in': ['MLB','NBA','NHL','NFL']}})
# handle_iter = sn.find({'_id': {'$in': ['penguins']}}) #Find individual teams

###########
##SCRAPER##
###########

for handle in handle_iter:

    print handle
    since_id_list = []

    if handle['since_id'] > 0:
        print handle['since_id']
        timeline = ty.Cursor(API.user_timeline, screen_name = handle['_id'], since_id=handle['since_id'], wait_on_rate_limit=True, wait_on_rate_limit_notify=True).items()
    else:
        timeline = ty.Cursor(API.user_timeline, screen_name = handle['_id'], wait_on_rate_limit=True, wait_on_rate_limit_notify=True).items()

    for tweet in timeline:

        time.sleep(.23)
        tweet._json['_id'] = tweet._json['id']
        tweet._json['datetime'] = datetime.strptime(tweet._json['created_at'], '%a %b %d %H:%M:%S +0000 %Y')
        try:
            print tweet._json['user']['screen_name']
            print tweet._json['text']
            tl.insert(tweet._json)
            since_id_list.append(tweet._json['_id'])

        except pymongo.errors.DuplicateKeyError:
            print 'dupe'
            since_id_list.append(tweet._json['_id'])

    print date
    print since_id_list
    try:
        sn.update({'_id': handle['_id']}, {'$set': {date: 1, 'since_id':max(since_id_list)}})

    except ValueError:
        print '0 new tweets' #if there are no new tweets

    print 'Completed @' + handle['_id']


#Function calls after scrape
dt.datetime_mongoinsert() #inserts datetimes for beginning and end
tli.aggregate_out(tl) #runs the aggregated script for engagement
tli.top_emoji_out(tl) #runs the emoji script