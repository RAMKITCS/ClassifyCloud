import os,io
# from difflib import get_close_matches
from google.cloud import storage
#os.environ['GOOGLE_APPLICATION_CREDENTIALS']=os.environ['gcp']
if 'gcp' in os.environ and os.environ['gcp'] is not None:
    os.environ['GOOGLE_APPLICATION_CREDENTIALS']=os.environ['gcp']
storage_client = storage.Client()
bucket_name=os.environ['bucket_name']
bucket = storage_client.bucket(bucket_name)
bucket_name2=os.environ['bucket_name']+"3"
bucket2 = storage_client.bucket(bucket_name2)
from google.cloud import vision
import time
import io,json
import mongoDB
# import nltk
# root=os.path.dirname(os.path.abspath(__file__))
# nltk_dir=os.path.join(root,"nltk_data")
# #print((nltk.data.path))
# nltk.data.path.append(nltk_dir)
# print((nltk.data.path))
# from nltk.corpus import stopwords 
# from nltk.tokenize import word_tokenize 
# stop_words = set(stopwords.words('english')) 
# from nltk.stem import WordNetLemmatizer
# from nltk.corpus import wordnet
def read_file(filename):
    blob=bucket.blob(filename)
    return blob.download_as_string()
def read_file_io(filename):
    blob=bucket.blob(filename)
    f=io.BytesIO()
    blob.download_to_file(f)
    f.seek(0)
    return f
def read_file_io_bucket2(filename):
    blob=bucket2.blob(filename)
    f=io.BytesIO()
    blob.download_to_file(f)
    f.seek(0)
    return f
def write_file_io(filename,content):
    blob=bucket.blob(filename)
    blob.upload_from_file(content)
    return 'completed'
def write_file(filename,content):
    blob=bucket.blob(filename)
    blob.upload_from_string(content)
    return 'completed'
def list_blob_all(prefix=None):
    blobs = storage_client.list_blobs(os.environ['bucket_name'],prefix=prefix)
    for blob in blobs:
        print(blob.name)
def download_to_local(filename,name):
    blob=bucket.blob(filename)
    blob.download_to_filename(name)
#for pre processing the data
#remove spl chars
# def remove_punctuation(text):
#     import os,string,re,json
#     text = ''.join([c if c not in string.punctuation else " " for c in text])
#     return text
# #for tagging the parts of speech 
# def pos_tagger(nltk_tag):
#     if nltk_tag.startswith('J'):
#         return wordnet.ADJ
#     elif nltk_tag.startswith('V'):
#         return wordnet.VERB
#     elif nltk_tag.startswith('N'):
#         return wordnet.NOUN
#     elif nltk_tag.startswith('R'):
#         return wordnet.ADV
#     else:         
#         return "n"
# def dataProcess(data):
#     data=data.lower()
#     filtered_sentence=remove_punctuation(data)
#     word_tokens = word_tokenize(filtered_sentence)
#     filtered_sentence = [w for w in word_tokens if not w in stop_words] 
#     lemmatizer = WordNetLemmatizer()
#     pos_tagged = nltk.pos_tag(filtered_sentence)
#     data=[lemmatizer.lemmatize(word,pos=pos_tagger(pos)) for word,pos in pos_tagged]
#     final_data=" ".join(data)
#     return final_data


def ocr_maker_1(pageinfo):
    main_ocr=pageinfo[2]
    rubber_main=pageinfo[1]
    pageinfo=pageinfo[0]
    print("pageinfo",pageinfo)
    bucket_name=os.environ['bucket_name']
    client = vision.ImageAnnotatorClient()
    print(client)
    st=time.time()
    image = vision.Image()
    value=pageinfo
    image.source.image_uri="gs://"+bucket_name+"/"+value
    print(value)
    #print(image)
    # Performs label detection on the image file
    response = client.document_text_detection(image=image)
    print(time.time()-st)
    # print(response)
    ocr=[]
    from collections import defaultdict
    rubber=defaultdict(list)
    documents=response.full_text_annotation
    for page in documents.pages:
        for block in page.blocks:
            for para in block.paragraphs:
                for word in para.words:
                    wor=""
                    for symbol in word.symbols:
                        wor+=symbol.text
                        
                        if (symbol.property.detected_break.type==1 or symbol.property.detected_break.type==3):
                            wor+=" "
                        if (symbol.property.detected_break.type==2):
                            wor+="\t"
                        if (symbol.property.detected_break.type==5):
                            wor+="\n"
                    ocr.append(wor)
                    rubber["1"].append([wor.strip(),word.bounding_box.vertices[0].x,word.bounding_box.vertices[2].x,word.bounding_box.vertices[0].y,word.bounding_box.vertices[2].y])
                    #print(symbol)
        rubber["img_H"].append(page.height)
        rubber["img_W"].append(page.width)
    #rubber_main[path]=rubber
    write_file(value+'_ocr.txt',"".join(ocr))
    rubber_main[pageinfo.split("/")[-1]]=rubber
    main_ocr["ocr"]+="".join(ocr)
    print('ocr_completed',pageinfo)
    del pageinfo,ocr,rubber
    import gc
    gc.collect()
    #return 'ocr-completed'
# def convert_pdf_to_image_split(data):
#     savepath,filename,parent=data[0],data[1],data[2]
#     import time
#     st=time.time()
#     images = convert_from_bytes(read_file_io(filename.replace("Contract","Contract_Input")).read(),poppler_path="")
#     pageinfo=dict()
#     for page in range(len(images)):
#         try:
#             page_byte=io.BytesIO()
#             print(page)
#             images[page].save(page_byte,"JPEG")
#             page_byte.seek(0)
#             write_file_io(savepath+filename.split('/')[-1]+f'__{page}.jpg',page_byte)
#             print(parent)
#             parent.send(savepath+filename.split('/')[-1]+f'__{page}.jpg')
#             pageinfo[page]=savepath+filename.split('/')[-1]+f'__{page}.jpg'
#             del page_byte
#         except Exception as e:
#             print(str(e))
#     write_file(savepath+'pageinfo.json',json.dumps(pageinfo))
#     parent.send("END")
#     print("split time",time.time()-st)
    #return pageinfo
import fitz
def convert_pdf_to_image_split(data):
    savepath,filename,parent=data[0],data[1],data[2]
    import time
    st=time.time()
    #images = convert_from_bytes(read_file_io(filename.replace("Contract","Contract_Input")).read(),poppler_path="")
    images = fitz.open("pdf",read_file_io_bucket2(filename.replace("Classification","Classification_Input")))
    pageinfo=dict()
    for page in range(len(images)):
        try:
            #page_byte=io.BytesIO()
            print(page)
            #pix=pg.get_pixmap()
            page_byte=images[page].get_pixmap(colorspace="gray").tobytes(output="jpg")
            write_file(savepath+filename.split('/')[-1]+f'__{page}.jpg',page_byte)
            print(parent)
            parent.send(savepath+filename.split('/')[-1]+f'__{page}.jpg')
            pageinfo[page]=savepath+filename.split('/')[-1]+f'__{page}.jpg'
            del page_byte
        except Exception as e:
            print(str(e))
    write_file(savepath+'pageinfo.json',json.dumps(pageinfo))
    parent.send("END")
    print("split time",time.time()-st)
    import gc
    gc.collect()


#pageinfo=convert_pdf_to_image_split('Contract/Residential-Lease-Agreement-4/','Contract/Residential-Lease-Agreement-4.pdf')