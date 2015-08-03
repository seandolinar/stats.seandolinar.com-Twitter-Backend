#-*- coding: utf-8 -*-
__author__ = 'seandolinar'

from pymongo import MongoClient
import socialmediaparse as smp
import json
from datetime import datetime
import io


client = MongoClient('box-jenkins.local', 27017)
db = client['sd_twitter']
sn = db['screen_name']
tl = db['engagement']

def dict_sort_list(dict):

    return sorted(dict, key=dict.get, reverse=True)




def aggregate_out(tl, date_str='Jul 1, 2015', filename='test.json'):

    date = datetime.strptime(date_str, '%b %d, %Y')

    team_out = []
    lgs = ['NBA','NHL','NFL','MLB']

    for lg in lgs:

        for handle in sn.find({'lg': lg}):

            print handle

            json_team_out = {}
            json_team_out['reply'] = 0
            json_team_out['rt'] = 0
            json_team_out['emoji'] = 0

            tweets = tl.find({ '$and': [{'user.screen_name': handle['_id']}, {'datetime': { '$gte': date }}]})

            for tweet in tweets:
                print tweet
                tweet_analysis = smp.TweetAnalysis(tweet)
                tweet_analysis.emoji()

                # json_out['id'] = tweet['_id']
                # json_out['datetime'] = tweet_analysis.datetime
                # json_out['handle'] = tweet_analysis.screen_name
                # json_out['reply'] = tweet_analysis.reply
                # json_out['rt_non_verified'] = tweet_analysis.rt_non_verified
                #
                # tweet_out.append(json_out)



                json_team_out['reply'] += tweet_analysis.reply
                json_team_out['rt'] += tweet_analysis.rt_non_verified
                json_team_out['emoji'] += tweet_analysis.emoji

                # print json_out
            json_team_out['handle'] = tweet_analysis.screen_name
            json_team_out['lg'] = lg
            json_team_out['count'] = tweets.count()
            json_team_out['date'] = date_str
            team_out.append(json_team_out)

        print team_out

    with open('../scratch/' + filename, 'w') as fp:
        json.dump(team_out, fp)




def top_emoji_out(tl, date_str='Jul 1, 2015', filename='emoji.json'):

    date = datetime.strptime(date_str, '%b %d, %Y')

    lgs = ['MLB','NBA','NHL']
    json_all_out = []
    for lg in lgs:

        for handle in sn.find({'lg': lg}):

            # print handle



            emoji_counter = smp.EmojiDict()

            tweets = tl.find({ '$and': [{'user.screen_name': handle['_id']}, {'datetime': { '$gte': date }}]})

            for tweet in tweets:
                print tweet['text']
                emoji_counter.add_emoji_count(tweet['text'])

            json_team = {u'handle': handle['_id'].encode('utf-8'), u'lg': lg.encode('utf-8'), u'begin_date': date_str, u'data': dict_sort_list(emoji_counter.dict)[:11]}
            json_all_out.append(json_team)

    with io.open('../scratch/' + filename, 'w', encoding='utf-8') as fp:
        data = json.dumps(json_all_out, ensure_ascii=False)
        fp.write(unicode(data))




if __name__ == '__main__':

    aggregate_out()
    top_emoji_out()


