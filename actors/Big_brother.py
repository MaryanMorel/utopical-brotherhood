#! /usr/bin/python2
# -*- coding: utf8 -*-

import pykka
import time
from Manager import Manager

class Big_Brother(pykka.ThreadingActor):
    def __init__(self):
        super(Big_Brother, self).__init__()
        self.pool = {'managers':[]}

    def start_manager(self, token):
        self.pool['managers'].append(Manager.start(token).proxy())
        return 0

    def stop_manager(self, token):
        answer = self.pool['managers'][w].stop_slaves()
        answer.get() # block thread
        self.pool['managers'][w].stop()
        return 0