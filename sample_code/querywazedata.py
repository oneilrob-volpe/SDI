import urllib.request
import os
import time
import datetime
import boto

# OPTIONAL: Import script that sets environment configuration variables
# import CONFIG

from boto.s3.key import Key
from boto.exception import S3ResponseError

# setup static variables
DOWNLOAD_LOCATION_PATH = './'       # use current directory

def downloadfiles(bucket_list):
    # download everything on the bucket list
    for file in bucket_list:
        key_string = str(file.key)
        s3_path = DOWNLOAD_LOCATION_PATH + key_string
        try:
            print("Currently downloading ", s3_path)
            file.get_contents_to_filename(s3_path)
        except (OSError, S3ResponseError) as e:
            pass
            # check if the file has been downloaded locally
            if not os.path.exists(s3_path):
                try:
                    head, tail = os.path.split(s3_path)
                    # create the folder
                    os.makedirs(head)
                    # and then make sure to re-attempt downloading the file
                    file.get_contents_to_filename(s3_path)
                except OSError as exc:
                    # let guard againts race conditions
                    import errno
                    if exc.errno != errno.EEXIST:
                        raise

s3 = boto.connect_s3(aws_access_key_id=os.environ['s3WazeAccessKey'], aws_secret_access_key=os.environ['s3WazeSecretKey'])

if s3.lookup(os.environ['s3WazeBucketName']) == None:
    print("Connection error or no such bucket!")

bucket = s3.get_bucket(os.environ['s3WazeBucketName'])

# display list of files that match the provided prefix.
files = bucket.list(prefix='MA/')                           # can change this to be whatever you are searching for
for key in files:
    print(key.name.encode('utf-8'))

# download all of the files
##downloadfiles(files)
