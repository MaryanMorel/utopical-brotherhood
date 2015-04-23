import pykka
import time
from sklearn.feature_extraction.text import CountVectorizer, TfidfTransformer
from sklearn.pipeline import Pipeline
from sklearn.cluster import SpectralClustering, AffinityPropagation

class Learner(pykka.ThreadingActor):
	"""Fetch friends and tweets for a given ego """
	def __init__(self, k=20):
		super(Learner, self).__init__()
        self.token_processer = Pipeline([('vect', CountVectorizer()), \
                                    ('tfidf', TfidfTransformer()) ])
        self.k = k # number of clusters
        self.clf = SpectralClustering(n_clusters=k, random_state=42, \
                            affinity='rbf', n_neighbors=15, eigen_tol=0.0)

    def learn(self, data):
        {'ego_id':self.ego_id ,'u_id':u_id, \
                                'u_document':" ".join(documents)}
        u_ids = []
        X = []
        def insert(elem):
            u_ids.append(elem['u_id'])
            X.append(elem[u_document])
        [insert(elem) for elem in data]
        # Create bag of words representation
        X = token_processer.fit_transform(X)
        labels = self.clf.fit_predict(X)

        clustering = [[] for i in range(k)]
        ## Fast loop:
        [ clustering[labels[i]].append(u_id) for i, u_id in enumerate(u_ids)]

        return {'ego_id':data.ego_id, clustering}