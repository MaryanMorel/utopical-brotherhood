#! /usr/bin/python2
# -*- coding: utf8 -*-

import pykka
import time
from Manager import Manager

class Big_Brother(pykka.ThreadingActor):
    def __init__(self):
        super(Beric, self).__init__()
        self.pool = {'managers':[]}

    def on_receive(self, message):
        if "start_manager" in message:
            token = message['start_manager'] ## Keep track of id / managers, to kill the right ones...
            # dump json or parse request message to fetch token
            self.pool['managers'].append(Manager.start(token))
            ## Broadcast message (INFO)
            return "Manager started" # check it !
        elif "stop_manager" in message:
            msg = message['update_message']
            w = msg[0]
            ## More intelligent way to kill workers ?
            ## Be elastic !
            self.pool['managers'][w].stop()
            ## Be sure that managers kill their workers !
            ## do : pykka.ActorRegistry.stop_all()
            ## in the server ?
            return "Manager killed" # check it !