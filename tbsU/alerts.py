#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Provides functionality for monitoring scripts and sending alerts through slack.
'''

import functools
import json
import os
import requests

from tabulate import tabulate

#####################################
# Classes                           #
#####################################

class Alerter:
    def __init__(self, username, emoji):
        self.emoji = emoji
        self.username = username


class AlertText:
    def __init__(self, text='', active=True):
        self.text = text
        self.active = active

    def __str__(self):
        return self.text

    def __repr__(self):
        return self.text


#####################################
# Constants                         #
#####################################

alerters_file = os.path.join(os.path.dirname(__file__), 'alerters.json')
slack_channels_file = os.path.join(os.path.dirname(__file__), './slack_channels.json')

default_alerters = {
    'alert': ['Alert', ':rotating_light:'],
    'mufasa': ['Mufasa', ':lion_face:'],
    'robot': ['Alert_Bot', ':robot_face:'],
    'skull': ['Stalfos', ':skull:'],
    'dinosaur': ['Rex', ':t-rex:'],
}


#####################################
# Helper Functions                 #
#####################################

def _alerters_conflict(stored, new_alerters):
    check = ''
    for k in new_alerters:
        if k in stored:
            if stored[k] != new_alerters[k]:
                check = input('This will overwrite one or more alerters. Continue? [y]/n')
                break
    
    return ('n' in check.lower())


def _mothball_alerter(a):
    if isinstance(a, list):
        return a
    elif isinstance(a, Alerter):
        return [a.username, a.emoji]
    else:
        err = 'To store an alerter, it must either be of class Alerter or a list of alerter properties'
        raise TypeError(err)


#####################################
# Main Functions                    #
#####################################

def alerters():
    adict = json.load(open(alerters_file))
    out = {k: Alerter(v[0], v[1]) for k, v in adict.items()}
    return out


def channels():
    cdict = json.load(open(slack_channels_file))
    return cdict 


def store_channel(channels):
    '''
    channels: dictionary
        Takes a dict of form {'channel_id': 'webhook_key'} and stores them for later use

    The webhook_key is simply the webhook url from slack. If you want to use alerters
        (i.e. custom icons/usernames) then I suggest using 
        slack.com/apps/A0F7XDUAZ-incoming-webhooks for creating your webhooks
    '''

    try:
        stored = json.load(open(slack_channels_file))
    except (NameError, FileNotFoundError):
        print('No stored channels found. Creating storage...')
        stored = {}
    
    stored = {**stored, **channels}

    with open(slack_channels_file, 'w') as outfile:  
        json.dump(stored, outfile)


def delete_channel(channels):
    '''
    channels: string or array-like
        List of channels to delete
    '''
    stored = json.load(open(slack_channels_file))

    if isinstance(channels, str):
        channels = [channels]

    for chan in channels:
        stored.pop(chan, None)

    with open(slack_channels_file, 'w') as outfile:  
        json.dump(stored, outfile)


def store_alerter(new_alerters={}, default=False):
    '''
    new_alerters: dictionary
        Takes a dict of form {'alerter_id': <alerter_definition>}, where alerter_definition
        is either an alerter object or a list of form ['alerter_username', 'alerter_emoji']

    default: Boolean 
        If true, adds the default package alerters instead of the given new_alerters dict
    '''
    try:
        stored = json.load(open(alerters_file))
    except (NameError, FileNotFoundError):
        print('No stored alerters found. Creating storage...')
        stored = {}

    if default:
        new_alerters = default_alerters
    
    if _alerters_conflict(stored, new_alerters):
        print("Ok, aborting")
        return

    new_alerters = {k: _mothball_alerter(v) for k, v in new_alerters.items()}

    stored = {**stored, **new_alerters}

    with open(alerters_file, 'w') as outfile:  
        json.dump(stored, outfile)


def delete_alerter(alerter_ids):
    '''
    alerter_ids: string or array-like
        list of alerter_ids to delete from storage
    '''
    stored = json.load(open(alerters_file))

    if isinstance(alerter_ids, str):
        alerter_ids = [alerter_ids] 

    for aid in alerter_ids:
        stored.pop(aid, None)

    with open(alerters_file, 'w') as outfile:  
        json.dump(stored, outfile)


def slack_msg_df(text, df, tablefmt='psql'):
    title = text
    body = tabulate(df, headers='keys', tablefmt=tablefmt)
    return '\n```'.join([title, body, ''])


def alert(text, channel=None, alerter=None, webhook_key=None):
    ''' Sends an alert usings guitly spark to a slack channel '''

    if not webhook_key:
        if not channel:
            raise ValueError('One of channel or webhook key must be defined.')
        webhook_key = channels()[channel]

    pl_dict = {'text': text}

    if alerter:
        if isinstance(alerter, str):
            alerter = alerters()[alerter]
        pl_dict['icon_emoji'] = alerter.emoji
        pl_dict['username'] = alerter.username

    payload = json.dumps(pl_dict)
    header = {'Content-type': 'application/json'}
    
    return requests.post(webhook_key, data=payload, headers=header)


def alert_monitor(text, channel=None, alerter=None, webhook_key=None):
    ''' 
    a decorator for monitoring functions 
    
    e.g:

    @alert_monitor('my message here')
    def hello(a):
        print(a/0)
    '''

    def func_decorator(func):

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                func(*args, **kwargs)
            except:
                if isinstance(text, AlertText) and text.active:
                    alert(text.text, channel, webhook_key, alerter)
                else:
                    alert(text, channel, webhook_key, alerter)
                raise

        return wrapper

    return func_decorator


#####################################
# Initialize                        #
#####################################

if not os.path.isfile(alerters_file):
    store_alerters(default=True)

if not os.path.isfile(alerters_file):
    store_channel({})









