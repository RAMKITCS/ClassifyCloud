import pymongo,os
from pymongo import MongoClient
from urllib.parse import quote_plus
import uuid
import bson.objectid
from datetime import datetime
import json
import certifi
db_cred={}
exec("def DBCred():\n\timport os\n\tfrom urllib.parse import quote_plus\n\treturn 'mongodb+srv://'+quote_plus('classify')+':'+quote_plus(os.getenv('mongopass'))+'@cluster0.raf2mvt.mongodb.net/?retryWrites=true&w=majority'",db_cred)
print(db_cred.keys())
def my_handler(x):
    if isinstance(x, datetime):
        return x.isoformat()
    elif isinstance(x, bson.objectid.ObjectId):
        return str(x)
    else:
        raise TypeError(x)
def Connection():
    cluster=MongoClient(db_cred['DBCred'](), tlsCAFile=certifi.where())
    db=cluster["Classification"]
    collections=db["Documents"]
    return collections
def Connection2():
    cluster=MongoClient(db_cred['DBCred'](), tlsCAFile=certifi.where())
    db=cluster["contract"]
    collections=db["metainfo"]
    return collections
def LoginConnection():
    cluster=MongoClient(db_cred['DBCred'](), tlsCAFile=certifi.where())
    db=cluster["Classification"]
    collections=db["login"]
    return collections
def getMetaInfo():
    return json.dumps(list(Connection2().find()),default=my_handler)
def addMetaInfo(query):
    if 'key_clause' in query:
        query['key_clause']=query['key_clause'].split(',')
    return Connection2().insert_one(query)
def editMetaInfo(id,query):
    print(id,query)
    if 'key_clause' in query:
        query['key_clause']=query['key_clause'].split(',')
    return Connection2().update_one({'_id':bson.objectid.ObjectId(id)},{'$set':query})
def insert(query):
    return Connection().insert_one(query)
def updateReturn(filter,data):
    return Connection().find_one_and_update(filter,{'$set':data}) 
# post={"name":"b","upload_date":datetime.now().strftime(("%d/%m/%Y %H:%M:%S")),"queue":"Scan","status":"On Queue","doc_type":"Lease"}
# #res=collections.find_one({"name":"b"})
# res=insert({"name":"b","upload_date":datetime.now().strftime(("%d/%m/%Y %H:%M:%S")),"queue":"Scan","status":"On Queue","doc_type":"Lease"}
# )
def getAllMeta():
    return list(Connection2().find()) 
def deleteMeta(query):
    query=[bson.objectid.ObjectId(k) for k in query]
    return Connection2().delete_many({"_id":{"$in":query}})
def delete_all():
    return Connection2().delete_many({})
def findall_json(date=None):
    if date:
        import bson
        #regx = bson.regex.Regex('/'+date+'/')
        return json.dumps(list(Connection().find({'upload_date':{'$regex':date}})),default=my_handler)
    return json.dumps(list(Connection().find()),default=my_handler)
def update(id,que,doc_type):
    return Connection().update_one({'_id':id},{"$set":{'queue':que,'doc_type':doc_type}})
def delete(query):
    return Connection().delete_many(query)

#login check
def login_check(username,password):
    return LoginConnection().find_one({"username":username,"password":password})
#print(login_check("yt","tccs"))
#print(delete({"name":"b"}))
# print((res))

