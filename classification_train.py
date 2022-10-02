from nltk import WordNetLemmatizer,word_tokenize
def clean(text):
    word_lem=WordNetLemmatizer()
    tokens=word_tokenize(text)
    lower=[word.lower() for word in tokens if len(word)>2 and word.isalpha()]
    lemmatized_text=[word_lem.lemmatize(word) for word in lower]
    return lemmatized_text