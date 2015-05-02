#! /usr/bin/python2
# -*- coding: utf8 -*-

import zerorpc
from actors import Manager
import simplejson as json
import time

class Master(object):
    ## Simple server-server communication interface

    def __init__(self, app_token, mongodbconnection):
        self.app_token = app_token
        self.mongodbconnection = mongodbconnection
        self.pool = {'managers':[]}

    def start_manager(self, user_token):
        user_token = json.loads(user_token)
        self.pool['managers'].append(Manager.start(self.app_token, user_token, self.mongodbconnection).proxy())
        manager_id = len(self.pool['managers']) - 1
        return manager_id #id of the manager

    def start_manager_and_run(self, user_token):
        user_token = json.loads(user_token)
        self.pool['managers'].append(Manager.start(self.app_token, user_token, self.mongodbconnection).proxy())
        manager_id = len(self.pool['managers']) - 1
        ## Now update strategy for now, erase old data (blocking):
        ansr = self.pool['managers'][manager_id].erase_raw_data()
        ansr.get()
        ansr = self.pool['managers'][manager_id].erase_parsed_data()
        ansr.get
        ansr = self.pool['managers'][manager_id].erase_clusterings()
        ansr.get
        ## Run data extraction, parsing, and learning (non blocking)
        self.pool['managers'][manager_id].runAll()
        return manager_id #id of the manager

    ## Methods for fine control :
    def learn(self, manager_id_nb_clusters):
        manager_id_nb_clusters = manager_id_nb_clusters.split("_")
        manager_id = int(manager_id_nb_clusters[0])
        nb_clusters = int(manager_id_nb_clusters[1])
        self.pool['managers'][manager_id].learn(nb_clusters)

    def erase_manager_raw_data(self, manager_id):
        manager_id = int(manager_id)
        self.pool['managers'][manager_id].erase_raw_data()

    def erase_manager_parsed_data(self, manager_id):
        manager_id = int(manager_id)
        self.pool['managers'][manager_id].erase_parsed_data()

    def erase_manager_clusterings(self, manager_id):
        manager_id = int(manager_id)
        self.pool['managers'][manager_id].erase_clusterings()

    ## Cleaning :
    def stop_manager(self, manager_id):
        manager_id = int(manager_id)
        answer = self.pool['managers'][manager_id].stop_slaves()
        answer.get() # block thread
        self.pool['managers'][manager_id].stop()

    def sleep_and_wake(self, sleep_time):
        ##  method sleep user send nb sec for testing purpose
        time.sleep(float(sleep_time))
        return "Thanks for the nap, dude !"

if __name__ == '__main__':
    ## Read token
    with open("../utopical.settings", "r") as infile:    
        settings = json.load(infile)
    app_token = {'key':settings['twitter']["app_key"], 'secret':settings['twitter']["app_secret"]}
    mongodbconnection = {'host':settings['mongodb']['host']}
    ## Configure server
    server = zerorpc.Server(Master(app_token, mongodbconnection)) ## Expose manager interface
    server.bind("tcp://0.0.0.0:4242") # Listen all ips on port 4242 for tcp connections
    ## Run server
    server.run()