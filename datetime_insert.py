#-*- coding: utf-8 -*-
__author__ = 'seandolinar'

import time
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import tweepy as ty
from pymongo import MongoClient
import pymongo
import LIB_race_emoji as lb
from datetime import datetime


client = MongoClient('box-jenkins.local', 27017)
db = client['twitter']
sn = db['screen_name']
tl = db['diversity_test10']


def datetime_mongoinsert():

    handle_iter = sn.find({})

    dates = []

    for handle in handle_iter:

        print handle
        tweets = tl.find({'user.screen_name': handle['_id']}, {'created_at': 1})
        for tweet in tweets:

            date = datetime.strptime(tweet['created_at'], '%a %b %d %H:%M:%S +0000 %Y')
            dates.append(date)

        if len(dates) > 0:
            sn.update({'_id': handle['_id']}, {'$set':{'begin_date': min(dates), 'end_date': max(dates), 'count': tweets.count()}}, upsert=True)

        else:
            print '@' + handle['_id'] + ' is empty'



if __name__ == '__main__':

    datetime_mongoinsert()