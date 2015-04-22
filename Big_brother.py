import pykka
import time
from Manager import Manager

class Big_Brother(pykka.ThreadingActor):
    def __init__(self):
        super(Beric, self).__init__()
        self.pool = {'managers':[]}

    def on_receive(self, message):
    	if "start_manager" in message:
            token = message['start_manager']
            # dump json or parse request message to fetch token
    		self.pool['managers'].append(Manager.start(token))
            # Get future : number of urls / tweets
            # Start other workers according to this info ?
            # Make the other workers send message when they're done ?
    		return "manager started" # check it !
    	elif "stop_manager" in message:
    		msg = message['update_message']
    		w = msg[0]
            ## More intelligent way to kill workers ?
            ## Be elastic !
    		self.pool['managers'][w].stop()
            ## Be sure that managers kill their workers !
    		return "Manager killed" # check it !