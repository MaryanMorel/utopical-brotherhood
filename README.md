# utopical-brotherhood

API that fetch twitter data and performs user clustering using NLP and spectral clustering. Based on ZeroRPC for server-server interaction and pykka actors for the data gathering, cleaning and for the learning. For now, english and french languages are supported.

Requires the use of MongoDB in order to store the results.

##### Important note : this is still an alpha version, many features and tests are not implemented yet.

### Dependencies : 
- zerorpc
- pykka
- pymongo
- tweepy
- nltk
- scikit-learn

### Settings :
The server needs the following informations contained in a file stored in ../ :
`{
	"twitter":{
		"app_key":"[twitter consumer key]",
		"app_secret":"[twitter consumer secret]",
		},
	"mongodb":{
			"host":"mongodb://[user]:[password]@[host]:[port]/[db_name]",
	}
}`

### Usage :
run Server.py

#### API : 
`start_manager(self, user_token)`: create a manager actor that supervise the data collection, cleaning and learning.

`start_manager_and_run(self, user_token)`: start a manager, and execute it (i.e. fetch and parse data, learn with nb_clusters=20). Erase previous results from the database (no history). Returns manager_id, the id of the manager.

`learn(self, manager_id, nb_clusters)`: performs a learning for a specified number of clusters, store the results in the DB.

`erase_manager_raw_data(self, manager_id)`: erase raw documents (tweets + urls) for a given user/manager.

`erase_manager_parsed_data(self, manager_id)`: erase parsed documents for a given user/manager. Parsing here means stemming + cleansing (remove punctuation)

`erase_manager_clusterings(self, manager_id)`: erase clustering results for a given user/manager.

`stop_manager(self, manager_id)`: kill the manager and its workers

`sleep_and_wake(self, sleep_time)`: make the manager sleep for sleep_time seconds and return an anwser. For testing purpose.

#### Examples :
Using command line interface (note the use of single quotes / double quotes in this precise order):
`$zerorpc tcp://[ip_of_the_server]:[port] start_manager '{"key":"[twitter_access_token_key]", "secret":"[twitter__access_token_secret]"}'`

`zerorpc tcp://127.0.0.1:14242 start_manager_and_run '{"key":"[twitter_access_token_key]", "secret":"[twitter__access_token_secret]"}'`

`zerorpc tcp://127.0.0.1:14242 learn 0_10` : learn 10 clusters for user corresponding to manager 0

Using python / node.js : cf. [zerorpc documentation](https://zerorpc.dotcloud.com/)

As zeroRPC is configured to listen to all incoming tcp requests, we advise to edit this setting in Server.py or to use a SSH tunnel for communications.


Note that `db_name`is hardcoded with the value `"utopical"` in the source code