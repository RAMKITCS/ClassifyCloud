import time,os,sys,re
import numpy as np
import pandas as pd
import pickle
import nltk
st=time.time()
# nltk.download('punkt')
# nltk.download('wordnet')
# nltk.download('stopwords')

import nltk
#nltk.download('omw-1.4',download_dir="nltk_data")
root=os.path.dirname(os.path.abspath(__file__))
nltk_dir=os.path.join(root,"nltk_data")
print((nltk.data.path))
#nltk.data.path.append(nltk_dir)
nltk.data.path=[nltk_dir]
print((nltk.data.path))
from nltk.corpus import stopwords
from nltk import WordNetLemmatizer,word_tokenize
from sklearn.model_selection import train_test_split
from sklearn.feature_extraction.text import CountVectorizer,TfidfTransformer,TfidfVectorizer
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score,precision_score,recall_score
import joblib
def clean(text):
    word_lem=WordNetLemmatizer()
    tokens=word_tokenize(text)
    lower=[word.lower() for word in tokens if len(word)>2 and word.isalpha()]
    lemmatized_text=[word_lem.lemmatize(word) for word in lower]
    return lemmatized_text
def vectorize(data,tfidf_vect_fit):
    x_tfidf=tfidf_vect_fit.transform(data)
    x_tfidf_df=pd.DataFrame(x_tfidf.todense(),columns=tfidf_vect_fit.get_feature_names())
    return x_tfidf_df
import joblib
def Predict(path,doc_type):
    try:
        from gcsconnect import read_file,download_to_local,write_file
        update_status=read_file("Classification/Models/status.txt").decode()
        if update_status=='1':
            write_file("Classification/Models/status.txt","0")
        if "model.pkl" not in os.listdir("models/") or update_status=='1':
            print("Downloading model pkl")
            download_to_local("Classification/Models/model.pkl","models/model.pkl")
        if "tfidf.pkl" not in os.listdir("models/") or update_status=='1':
            download_to_local("Classification/Models/tfidf.pkl","models/tfidf.pkl")
            print("Downloading tfidf pkl")
        model=joblib.load("models/model.pkl")
        tfidf_vect_fit=joblib.load("models/tfidf.pkl")
        ocr_data=read_file(path).decode()
        df2=pd.DataFrame({"data":[ocr_data]},dtype=str)
        pred_value=model.predict(vectorize(df2["data"].values.astype('U'),tfidf_vect_fit))
        print("predicted label",pred_value[0])
        doc_type["predicted_type"]=pred_value[0]
    except Exception as e:
        print(str(e))
        return str(e)
#print(Predict("Contract/A-2022-0921.Residential-Lease-Agreement-f5/A-2022-0921.Residential-Lease-Agreement-f5.pdf__0.jpg_ocr.txt"))