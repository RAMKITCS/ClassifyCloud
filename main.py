import pre_process
import os
from multiprocessing import Process
import functions_framework
try:
    print("main1",__name__,__file__)
except Exception as e:
    print("nltk error",str(e))
def call_pre(filename):
    p1=Process(target=pre_process.preprocess,args={filename,})
    p1.start()
    p1.join(3000)
    if p1.is_alive():
        print("Thread Alive")
        p1.terminate()
        print("Terminated the thread")
    print("completed")

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
#     import mongoDB
#     #print((mongoDB.Connection().find_one({"name":"test1.Residential-Lease-Agreement-f5.pdf"})))
#     call_pre("Classification_Input/input2.class_input.pdf")