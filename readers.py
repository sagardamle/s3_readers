from functools import wraps
import urllib
import tempfile
import boto3, botocore
import pandas as pd
import logging

## Reading
def s3reader(func):
    s3 = boto3.client('s3')
    @wraps(func)
    def inner1(*args, **kwargs):
        filename = args[0]
        logging.debug(f'Filename: {filename}')
        if filename.startswith('s3://'):
            parsed_url = urllib.parse.urlparse(filename)
            with tempfile.NamedTemporaryFile(delete = True) as tf:
                try:
                    s3.download_fileobj(parsed_url.netloc, parsed_url.path.strip('/'), tf)
                except botocore.exceptions.ClientError as ce:
                    print(f'Could not download file from s3: {parsed_url}. Check to see that it exists')

                tf.flush() # Have to flush here, else part of file may remain in buffer
                           # and won't be read by function.  This is necessary to do here
                           # because NamedTemporaryFile is being deleted after read
                return func(tf.name,*args[1:], **kwargs)
        return func(*args, **kwargs)
    return inner1

@s3reader
def read_hdf(*args, **kwargs):
    return pd.read_hdf(*args, **kwargs)

@s3reader
def read_csv(*args, **kwargs):
    return pd.read_csv(*args, **kwargs)
