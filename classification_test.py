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


def vectorize(data,tfidf_vect_fit):
    x_tfidf=tfidf_vect_fit.transform(data)
    x_tfidf_df=pd.DataFrame(x_tfidf.todense(),columns=tfidf_vect_fit.get_feature_names())
    return x_tfidf_df
import joblib
def Predict(data):
    path=data[0]
    doc_type=data[1]
    try:
        from gcsconnect import read_file,download_to_local,write_file
        update_status=read_file("Classification/Models/status.txt").decode()
        print("status",update_status)
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
        print("before predicted label")
        df_vect=vectorize(df2["data"].values.astype('U'),tfidf_vect_fit)
        pred_value=model.predict(df_vect)
        print("predicted label",pred_value[0])
        doc_type["predicted_type"]=str(pred_value[0])
        doc_type["predicted_score"]=str(model.predict_proba(df_vect).max()*100)
        print(str(model.predict_proba(df_vect).max()*100))
        del pred_value,df_vect
    except Exception as e:
        print("classification error",str(e))
        #return str(e)
#print(Predict("Contract/A-2022-0921.Residential-Lease-Agreement-f5/A-2022-0921.Residential-Lease-Agreement-f5.pdf__0.jpg_ocr.txt"))