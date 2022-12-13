from gensim.test.utils import common_texts
from gensim.models.doc2vec import Doc2Vec, TaggedDocument

class DocEmb:
    def __init__(self, model):
        self.model = model

    def __call__(self, doc):
        return self.model.infer_vector(doc)