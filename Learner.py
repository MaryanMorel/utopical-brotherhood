import pykka
import time
from sklearn.feature_extraction.text import CountVectorizer, TfidfTransformer
from sklearn.pipeline import Pipeline
from sklearn.cluster import SpectralClustering, AffinityPropagation

class Learner(pykka.ThreadingActor):
	"""Fetch friends and tweets for a given ego """
	def __init__(self):
		super(Learner, self).__init__()

    def on_receive(self, message):
        print("Hi ! I'm Learner !")
        time.sleep(1)
        # On some signal
        # run clustering for some user
        # push result into DB
        # return



# token_processer = Pipeline([('vect', CountVectorizer()),
#                             ('tfidf', TfidfTransformer())
#                             ])

# X = features
# #X = [" ".join(feature) for feature in features]
# # Create bag of words representation
# X = token_processer.fit_transform(X)

# # Quite good results for : 
# k=20 (Cross-validation ?)
# clf = SpectralClustering(n_clusters=k, random_state=42, affinity='rbf', n_neighbors=15, eigen_tol=0.0)

# labels = clf.fit_predict(X)
# #k = np.max(labels)
# res = []
# for i in range(k):
#     res.append([])
# for i in range(len(friends)):
#     res[labels[i]].append(friends[i]) ## Friends = screen_names list, same order as docs
# #pprint(res)

## Push into DB
# from datetime import datetime
# db['clusterings'].insert({"obj_type":"clustering", "nb_clusters":k, "content":{"clusters":res, "friends":friends, "nb_except":nb_except}, "created_at":str(datetime.now())})