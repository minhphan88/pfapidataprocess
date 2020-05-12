import nltk
from nltk.tokenize import RegexpTokenizer
from nltk.stem import WordNetLemmatizer,PorterStemmer
from nltk.corpus import stopwords
import re
from difflib import *
lemmatizer = WordNetLemmatizer()
stemmer = PorterStemmer()

def preprocess(sentence):
    sentence=str(sentence)
    sentence = sentence.upper()
    sentence=sentence.replace('{html}',"")
    cleanr = re.compile('<.*?>')
    cleantext = re.sub(cleanr, '', sentence)
    rem_url=re.sub(r'http\S+', '',cleantext)
    rem_num = re.sub('[0-9]+', '', rem_url)
    tokenizer = RegexpTokenizer(r'\w+')
    tokens = tokenizer.tokenize(rem_num)
    filtered_words = [w for w in tokens if len(w) > 2 if not w in stopwords.words('english')]
    stem_words=[stemmer.stem(w) for w in filtered_words]
    lemma_words=[lemmatizer.lemmatize(w) for w in stem_words]
    return filtered_words

#BuildingClassMap_Test.drop()
def upper_List(word_List):
    ret_List=[]
    for e in word_List:
        ret_List.appends(e.upper())
    return ret_List

def split_description_mapping(building_mapping):
    a=building_mapping
    for k,v in a.items():
        a[k]=v.split()
    return a

def empty_col_mapping(col_mapping):
    a=col_mapping
    for k,v in a.items():
        a[k]=[]
    return a



def historical_building_map(empty_sample,BuildingClassMap_Test):
    for k,v in empty_sample.items():
        string_K= preprocess(k)

        for doc in BuildingClassMap_Test.find({}):
            ratio = SequenceMatcher(None, string_K, doc["Words List"]).ratio()

            if ratio >= 0.7:
                v.append(doc["Code"])

    return empty_sample
