from importlib.resources import path
import nltk,os
download_dir = os.path.abspath("nltk_data")
os.makedirs(download_dir)
nltk.download('stopwords',download_dir=download_dir)
nltk.download('punkt',download_dir=download_dir)
nltk.download('wordnet',download_dir=download_dir)
nltk.download('averaged_perceptron_tagger',download_dir=download_dir)
nltk.download('omw-1.4',download_dir=download_dir)
root=os.path.dirname(os.path.abspath(__file__))
nltk_dir=os.path.join(root,"nltk_data")
print((nltk.data.path))
nltk.data.path=[nltk_dir]
print((nltk.data.path))