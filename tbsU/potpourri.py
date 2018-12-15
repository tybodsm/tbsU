import base64
import datetime
import pandas as pd
import re

def camel2snake(string):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', string)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def clean_string(string):
    string = re.sub(r'(^\s+)|(\s+$)', '', string)
    string = string.replace(' ', '_').replace('&', 'And')
    string = re.sub(r'[^a-zA-Z0-9_]', '', string)
    string = camel2snake(string)
    string = re.sub(r'_+', '_', string)
    return string


def encode64_epoch(dt=None):
    '''
    Stores them as 6-character URL-safe strings. Disregards milliseconds. 
    Not perfectly efficient, but simple and effective. Works for dates 1925 to ~2061. 
    '''
    dt = dt or datetime.datetime.now()
    seconds = int(dt.timestamp()) + 1420070400  # rebase to 1925-01-01

    byte_enc = seconds.to_bytes(4, byteorder='big', signed=False)
    b_string = base64.b64encode(byte_enc, altchars=b'-_')
    out = b_string.decode('ascii')[:6]
    return out


def decode64_epoch(string, format_pandas=True):
    '''
    Decodes strings produced by the encode64_epoch function. 
    '''
    b_string = (string + '==').encode()
    byte_enc = base64.b64decode(b_string, altchars=b'-_')
    epoch = int.from_bytes(byte_enc, byteorder='big', signed=False) - 1420070400  #rebase to epoch
    if format_pandas:
        out = pd.to_datetime(epoch, unit='s')
    else:
        out = datetime.datetime.utcfromtimestamp(epoch)

    return out







