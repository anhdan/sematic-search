
from gensim.test.utils import common_texts
from gensim.models.doc2vec import Doc2Vec, TaggedDocument
import pprint

documents = [TaggedDocument(doc, [i]) for i, doc in enumerate(common_texts)]
model = Doc2Vec(documents, vector_size=500, window=2, min_count=1, workers=4)


vector = model.infer_vector(["system", "response"])
print(vector)


