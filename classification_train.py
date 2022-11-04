from nltk import WordNetLemmatizer,word_tokenize
from nltk.corpus import stopwords
stop=stopwords.words('english')
def clean(text):
    word_lem=WordNetLemmatizer()
    tokens=word_tokenize(text)
    lower=[word.lower() for word in tokens if len(word)>2 and word.isalpha() and word not in stop]
    lemmatized_text=[word_lem.lemmatize(word) for word in lower]
    #print(lower)
    return lemmatized_text