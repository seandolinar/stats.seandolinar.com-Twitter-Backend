#-*- coding: utf-8 -*-
__author__ = 'seandolinar'


# tweepy setup
import time
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import tweepy as ty
import random
import json
import socialmediaparse as smp

# mongo setup
from pymongo import MongoClient
import pymongo
# import LIB01_Teams as lb
from datetime import datetime

client = MongoClient('box-jenkins.local', 27017) #'192.168.1.100'
db = client['sd_twitter']
sn = db['screen_name']
# tl = db['engagement']


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

def fav_scraper(fav, API, reset=False):

    handle_iter = sn.find({'lg': 'NHL'})
    # handle_iter = sn.find({'_id': {'$in': ['Pirates']}})

    for handle in handle_iter:

        for j in range(0,17):

            favorites = API.favorites(screen_name=handle['_id'], count=200, page=j, wait_on_rate_limit=True)

            try:
                for tweet in favorites:

                    tweet_json = tweet._json
                    tweet_json['team'] = handle['_id']
                    tweet_json['datetime'] = datetime.strptime(tweet_json['created_at'], '%a %b %d %H:%M:%S +0000 %Y')
                    tweet_json['_id'] = tweet_json['id']

                    fav.insert(tweet_json)
                    print tweet_json


            except pymongo.errors.DuplicateKeyError:

                print 'dupe'
                if reset:
                    continue
                else:
                    print 'exiting'
                    break

        time.sleep(62)

def fav_aggregate_out(fav, date='Jul 1, 2015', filename='fav_test.json'):

    date = datetime.strptime(date, '%b %d, %Y')

    team_out = []
    lgs = ['NBA','NHL','NFL','MLB']
    json_team_out = []

    with open('../scratch/' + 'test.json', 'r') as fav_file:

        data_dict = json.load(fav_file)

        for lg in lgs:

            for handle in sn.find({'lg': lg}):

                print handle



                tweets = fav.find({ '$and': [{'team': handle['_id']}, {'datetime': { '$gte': date }}]})

                for i in range(len(data_dict)):
                    if data_dict[i]['handle'] == handle['_id']:
                        data_dict[i]['fav'] = tweets.count()

            print data_dict

        # with open('../scratch/' + filename, 'w') as fp:
        #     json.dump(team_out, fp)




fav_scraper(fav,API, reset=True)
fav_aggregate_out(fav, 'Jan 1, 2015')