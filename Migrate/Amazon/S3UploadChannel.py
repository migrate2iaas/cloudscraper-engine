import boto
from boto.s3.connection import S3Connection
from boto.s3.key import S3Key

import threading
import Queue

class S3UploadThread(threading.Thread):

    def __init__(self , queue):
        self.__uploadQueue = queue
        return 
    def run(self):
        workitem = self.__uploadQueue.get()

        self.__uploadQueue.task_done()
        #the upload activity

#TODO: inherit from kinda base one
class S3UploadChannel(object):
    """description of class"""


    def __init__(self, bucket, imagename, awskey, awssercret , uploadThreads=4):
        self.__uploadQueue = Queue.Queue()
        #TODO: initializing a number of threads
        return

    # this one is async
    def uploadData(self, start, size, data_getter):
       
        s3key = S3Key(bucket , "partN")
        set_contents_from_string(s, headers=None, replace=True, cb=None, num_cb=10, policy=None, md5=None, reduced_redundancy=False, encrypt_key=False)¶

        return 

    #gets the overall size of data uploaded
    def getOverallDataTransfered():
        return

    # wait uploaded all needed
    def waitTillUploadComplete():
        self.__uploadQueue.join()
        return

    # confirm good upload. uploads resulting xml then
    def confirm():
        # generate the XML file then:

        # for every file part uploaded, do: 
        s3 = S3Connection('YOUR_KEY', 'YOUR_SECRET', is_secure=False)
        url = s3.generate_url(60, 'GET', bucket='YOUR_BUCKET', key='YOUR_FILE_KEY', force_http=True)
        
        # start the conversion task here!?
        # or implementations should differ in case of different system\volume data types?
        return