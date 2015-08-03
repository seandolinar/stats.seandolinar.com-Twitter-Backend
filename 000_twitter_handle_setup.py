#-*- coding: utf-8 -*-
__author__ = 'seandolinar'

from pymongo import MongoClient
import pymongo
import LIB_race_emoji as lb


client = MongoClient('box-jenkins.local', 27017)
db = client['sd_twitter']
sn = db['screen_name']
collection = db['engagement']


#reset
reset = False


#lg_list = lb.team_handles.keys()
lg_list = ['NBA','NHL','MLB','NFL']

for lg in lg_list:


    #handle list
    team_list = lb.team_handles[lg]


    #creates a database of screen names so program can resume if neccessary
    for handle in team_list:

        tweets = collection.find({'user.screen_name': handle}, {'_id':1})
        id_list = []

        for tweet in tweets:
            id_list.append(tweet.get('_id'))
        if len(id_list) > 0:
            since_id = max(id_list)
        else:
            since_id = 0


        print handle
        sn.update({'_id': handle}, {'$set': {'lg': lg, 'since_id': since_id}}, upsert=True)



