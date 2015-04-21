import pykka
import time
import warnings
from Fetcher import Fetcher
from Parser import Tw_parser, Url_parser
from time import sleep

class Manager(pykka.ThreadingActor):
    """ Manage one user + communicate with DB """
    def __init__(self, app_token, user_token):
        super(Manager, self).__init__()
        self.app_token = app_token
        self.user_token = user_token
        self.pool = {'tw_fetcher':None, 'f_fetcher':None, 'tw_parsers':[], 'url_parsers':[], 'learners':[]}
        # Create connections
        auth = tweepy.OAuthHandler(app_token["key"], app_token["secret"])
        auth.set_access_token(user_token["key"], user_token["secret"])
        # self.api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
        self.api = tweepy.API(auth)

        # get ego
        ego = self.api.me()
        self.ego_screen_name = ego.screen_name
        self.ego_friends_count = ego.friends_count

        # Get friends ids list
        friends_ids = []
        for page in tweepy.Cursor(api.friends_ids, screen_name=res.screen_name).pages():
            friends_ids.extend(page)
        self.friends_ids = set(friends_ids)
        if len(friends_ids) < self.ego_friends_count:
            warnings.Warn('Did not get all the friends ids of %s' %self.ego_screen_name)
            ## Do something to handle this case
        self.remaining_friends = None
        self.remaining_timelines = None

    def on_receive(self, message):
        if "fetch_friends" in message:
            self.remaining_friends = None
            self.pool['f_fetcher'] = Fetcher.start(self.app_token, self.user_token)
            answer = self.pool['f_fetcher'].ask( \
                {"fetch_friends":{'ids':self.friends_ids}}, timeout=180)
            if answer["status"] == 0:
                ######
                ## push in mongoDB (friends + ego_id)
                ## {ego_id: 314231332, friends_ids:[], friends_scr_names:[]} ?
                ## What about lang + description ?
                ## What if we fetch users + their tweets at the same time ?
                ## Would be easier, just "fetch_data"
                ######
            elif answer["status"] == 0xDEADFEED:
                # Timeout
                while answer["status"] == 0xDEADFEED:
                    ######
                    ## push retrieved friends in mongoDB
                    ######
                    self.remaining_friends = answer["unprocessed_friends"]
                    sleep(300) # Wait 5 min
                    answer = self.pool['f_fetcher'].ask( \
                        {"fetch_friends":{'ids':self.remaining_friends}}, timeout=180)
            # else:
                # raise ?
            self.pool['f_fetcher'].stop()
            ## send info msg
            out_msg = "Friends of %s fetched"%self.ego_screen_name

        elif "friends_tweets" in message:
            self.remaining_friends = None
            self.pool['tw_fetcher'] = Fetcher.start(self.app_token, self.user_token)
            answer = self.pool['tw_fetcher'].ask( \
                {"fetch_tweets":{'ids':self.friends_ids}}, timeout=180)
            if answer["status"] == 0:
                ######
                ## push in mongoDB (timelines + ego_id)
                ######
            elif answer["status"] == 0xDEADFEED:
                # Timeout
                while answer["status"] == 0xDEADFEED:
                    ######
                    ## push retrieved timelines in mongoDB
                    ######
                    self.remaining_friends = answer["unprocessed_friends"]
                    sleep(300) # Wait 5 min
                    answer = self.pool['tw_fetcher'].ask( \
                        {"fetch_tweets":{'ids':self.remaining_friends}}, timeout=180)
            # else:
                # raise ?
            self.pool['tw_fetcher'].stop()
            ## send info msg
            out_msg = "Tweets of %s's friends fetched"%self.ego_screen_name

        elif "parse_data" in message:
            # Create parser and interact with it
            out_msg = "Documents of %s's friends created"%self.ego_screen_name
            # Save it to DB

        elif "update_data" in message:
            # update friends + tweets + documents
            # save it to DB

        elif "learn" in message:
            # Read data from DB
            # run clustering then save it to DB
        else :
            out_msg = 0xBAADF00D
        return out_msg