import multiprocessing
from multiprocessing import managers
import gcsconnect
import os,json
import mongoDB
from multiprocessing import Process,Pipe 
from datetime import datetime   
def preprocess(path):
    #result=mongoDB.updateReturn({'queue':'Scan'},{'queue':'In Progress'})
    filename=path.split("/")[-1]
    path=path.replace("Classification_Input","Classification")
    result=mongoDB.updateReturn({'name':filename,'queue':'Scan'},{'queue':'In Progress'})
    print(result)
    if result:
        #path='Contract/'+result['name']
        #path=filename
        parent,child=Pipe() 
        #p=Process(target=preprocess,args={(path,result['_id'],parent),})
        rubber_main=multiprocessing.Manager().dict()
        main_ocr=multiprocessing.Manager().dict()
        main_ocr["ocr"]=""
        p2=Process(target=worker_pre,args={(child,rubber_main,main_ocr),})
        p2.start()
        
        print(path)
        #pageinfo=convert_pdf_to_image_split('Contract/Residential-Lease-Agreement-4/','Contract/Residential-Lease-Agreement-4.pdf')
        import time
        st=time.time()
        flag=True
        p=Process(target=gcsconnect.convert_pdf_to_image_split,args={(path.rstrip('.pdf')+'/',path,parent),})
        p.start()
        p.join(200)
        if p.is_alive():
            flag=False
            print("Splitter Thread Alive")
            parent.send("END")
            p.terminate()
            print("Terminated the splitter thread")
        #print(pageinfo)
        p2.join(400)
        if p2.is_alive():
            flag=False
            print("OCR Thread Alive")
            p2.terminate()
            parent.close()
            child.close()
            print("Terminated the OCR thread")
        try:
            gcsconnect.write_file(path.rstrip('.pdf')+"/main_ocr.txt",main_ocr.copy()["ocr"])
            gcsconnect.write_file(path.rstrip('.pdf')+"/rubber.json",json.dumps(rubber_main.copy()))
        except Exception as e:
            print("Rubber or Ocr file Error",str(e))
        del rubber_main,main_ocr
        print("ocr generation done")
        if flag:
            print("Classification started")
            import classification_test
            doc_type=multiprocessing.Manager().dict()
            p3=Process(target=classification_test.Predict,args={(path.rstrip('.pdf')+"/main_ocr.txt",doc_type)})
            p3.start()
            p3.join(100)
            if p3.is_alive():
                flag=False
                print("Classification Thread Alive")
                p3.terminate()
                print("Terminated the Classification thread")
            print("Classification finished")
            #print(gcsconnect.ocr_maker(pageinfo))
            if flag:
                doc_type=doc_type.copy()
                mongoDB.update(result['_id'],'Completed',str(doc_type.get("predicted_type")),str(doc_type.get("predicted_score")))
            del doc_type
        if not flag:
            mongoDB.Connection().update_one({'_id':result['_id']},{"$set":{'queue':"Exception","completed_date":datetime.now().strftime(("%d/%m/%Y %H:%M:%S"))}})
        print("pre process time",time.time()-st)
        #mongoDB.update(id,'Validation1','Ready',doc_type.copy()["predicted_type"])
        print('updated')
    else:
        print("all done")
def worker_pre(receiver):
    rubber_main=receiver[1]
    main_ocr=receiver[2]
    receiver=receiver[0]
    print('workerpre started')
    while 1:
        msg=receiver.recv()
        print(os.getpid())
        if msg=="END":
            print("end")
            break
        print("received msg : ",msg,os.getpid())
        p2=Process(target=gcsconnect.ocr_maker_1,args={(msg,rubber_main,main_ocr),})
        p2.start()
        p2.join()