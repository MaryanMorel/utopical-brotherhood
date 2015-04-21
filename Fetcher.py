from __future__ import division
from math import ceil
import pykka
import time
from warnings import warn

class Fetcher(pykka.ThreadingActor):
    """Fetch friends and tweets for a given ego
    On actor per user must be created """
    # Create a new actor for each user
    # insert into DB ? -> Yes, seems reasonable (in case of crash)
    # send to parsers -> There is a queuing process, might be ok 
    ###(assumption: nobody follows more than 10k dudes)
    def __init__(self, app_token, user_token):
        super(Fetcher, self).__init__()
        # Create connection
        auth = tweepy.OAuthHandler(app_token["key"], app_token["secret"])
        auth.set_access_token(user_token["key"], user_token["secret"])
        self.api = tweepy.API(auth)
        # get ego
        ego = self.api.me()
        self.ego_screen_name = ego.screen_name

    def on_receive(self, message):
        if "fetch_friends" in message:
            ids = message['fetch_friends']['ids']
            out_msg = self.fetch_friends(ids)
        elif "fetch_timelines" in message:
            ids = message['fetch_timelines']['ids']
            out_msg = self.fetch_tweets(ids)
        else :
            warn('0xBAADF00D')
            out_msg = {'status':0xBAADF00D}
        return out_msg

    def fetch_friends(self, ids):
        ## get friends with GET users/lookup
        # Get pages of friends, 100 friends per request, 180 request / 15 min
        friends = []
        processed = []
        try :
            n_ids = len(ids)
            n_requests = ceil(len(ids)/100)
            for i in range(n_requests):
                l = i * 100
                u = min(l + 99, n_ids)
                bulk = api.lookup_users(user_ids=ids[l:u])
                friends.extend([clean_friend(f) for f in bulk])
                processed.extend(ids[l:u])

            out_msg = {'status':0, 'friends':friends \
                        'unprocessed_friends':None}

        except tweepy.TweepError as exc:
            warn('0xDEADFEED')
            warn(exc)
            unprocessed_friends = set(ids).difference(set(processed))
            out_msg = {'status':0xDEADFEED, 'friends':friends \
                        'unprocessed_friends':unprocessed_friends}

        return out_msg

    def fetch_tweets(self, ids):
        ## GET statuses/user_timeline
        # up to 3,200 of a user’s most recent Tweets, 180 requests / 15 min
        processed = []
        timelines = {}
        try:
            for fid in ids:
                timelines[fid] = api.user_timeline(id=fid, count=100) # Fetch 100 last tweets
                processed.append(fid)

            out_msg = {'status':0, 'timelines':timelines \
                        'unprocessed_friends':None}

        except tweepy.TweepError as exc:
            warn('0xDEADFEED')
            warn(exc)
            unprocessed_friends = set(ids).difference(set(processed))
            out_msg = {'status':0xDEADFEED, 'timelines':timelines \
                        'unprocessed_friends':unprocessed_friends}

        return out_msg

    def clean_friend(self, friend):
        return {'id':f.id, 'screen_name':f.screen_name, 'lang':f.lang, 'description':f.description}


