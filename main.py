import pre_process
import os
from multiprocessing import Process
import functions_framework
try:
    print("main1",__name__,__file__)
except Exception as e:
    print("nltk error",str(e))
def call_pre(path):
    import mongoDB
    filename=path.split("/")[-1]
    result=mongoDB.Connection().find_one({'name':filename,'queue':'Scan'})
    print(result)
    try:
        p1=Process(target=pre_process.preprocess,args={path,})
        p1.start()
        p1.join(520)
        if p1.is_alive():
            print("Thread Alive")
            p1.terminate()
            print("Terminated the thread")
            from datetime import datetime
            mongoDB.Connection().update_one({'_id':result['_id']},{"$set":{'queue':"Exception","completed_date":datetime.now().strftime(("%d/%m/%Y %H:%M:%S"))}})
        print("completed")
    except Exception as e:
        print("Application Error",str(e))
        mongoDB.Connection().update_one({'_id':result['_id']},{"$set":{'queue':"Exception","completed_date":datetime.now().strftime(("%d/%m/%Y %H:%M:%S"))}})

@functions_framework.cloud_event
def hello_gcs(cloud_event):
    data = cloud_event.data

    event_id = cloud_event["id"]
    event_type = cloud_event["type"]

    bucket = data["bucket"]
    name = data["name"]
    metageneration = data["metageneration"]
    timeCreated = data["timeCreated"]
    updated = data["updated"]
    print("main name:",__name__)
    print(f"Event ID: {event_id}")
    print(f"Event type: {event_type}")
    print(f"Bucket: {bucket}")
    print(f"File: {name}")
    print(f"Metageneration: {metageneration}")
    print(f"Created: {timeCreated}")
    print(f"Updated: {updated}")
    call_pre(name)
    print("Event Completed",name)
# if __name__=="__main__":
#     #import mongoDB
#     #print((mongoDB.Connection().find_one({"name":"test1.Residential-Lease-Agreement-f5.pdf"})))
#     call_pre("Classification_Input/v1.URLA_2019_Borrower_v28.pdf")