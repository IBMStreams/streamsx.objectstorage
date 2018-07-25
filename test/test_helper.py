# Copyright (C) 2018 International Business Machines Corporation. 
# All Rights Reserved.

import sys, os, time
from subprocess import call, Popen, PIPE
import json
import streamsx.rest

# environment variable for COS credential file
def COS_CREDENTIALS():
    return "COS_CREDENTIALS"

# environment variable for COS IAM credential file
def COS_IAM_CREDENTIALS():
    return "COS_IAM_CREDENTIALS"

def run_shell_command_line(command):
    process = Popen(command, universal_newlines=True, shell=True, stdout=PIPE, stderr=PIPE)
    stdout, stderr = process.communicate()
    return stdout, stderr, process.returncode

def parseApplicationTrace(logfile, srchString):
    cmd = "tar -Oxvzf " + logfile + " | grep '" + srchString + "'"
    stdout, stderr, rc = run_shell_command_line(cmd)
    if stdout != '':
        res = stdout[stdout.index(srchString)-2:]
    else:
        res = ""
    return res

def exec_noexit(seq):
    p = Popen(seq, stdout=PIPE, stderr=PIPE)
    stdout, stderr = p.communicate()
    return stdout, stderr, p.returncode

def read_iam_credentials():
    iam_api_key = ""
    service_instance_id = ""
    try:
        cred_file = os.environ[COS_IAM_CREDENTIALS()]
        print("COS IAM credentials file:" + cred_file)
        with open(cred_file) as data_file:
            credentials = json.load(data_file)
        #print("Service credential:")
        #print(json.dumps(credentials, indent=2))
        #print("")

        api_key = credentials.get('apikey')
        resource_instance_id = credentials.get('resource_instance_id')
        # need to extract the last part of the resource_instance_id for ObjectStorage toolkit operators
        data = resource_instance_id.split(":")
        for temp in data:
            if temp != '':
                service_instance_id = temp
        print("service_instance_id:"+service_instance_id)
        
    except KeyError: 
        print("Environment variable "+COS_IAM_CREDENTIALS()+" is not set.")

    return api_key, service_instance_id

def read_credentials():
    access_key = ""
    secret_access_key = ""
    try:
        cred_file = os.environ[COS_CREDENTIALS()]
        print("COS credentials file:" + cred_file)
        with open(cred_file) as data_file:
            credentials = json.load(data_file)
        #print("Service credential:")
        #print(json.dumps(credentials, indent=2))
        #print("")
        access_key = credentials.get('access_key')
        secret_access_key = credentials.get('secret_access_key')        
    except KeyError: 
        print("Environment variable "+COS_CREDENTIALS()+" is not set.")

    return access_key, secret_access_key

def cos_credentials():
    result = True
    try:
        os.environ[COS_CREDENTIALS()]
    except KeyError: 
        result = False
    return result

def iam_credentials():
    result = True
    try:
        os.environ[COS_IAM_CREDENTIALS()]
    except KeyError: 
        result = False
    return result

def start_streams_cloud_instance():
    print ("START Streaming Analytics service instance ...")
    # start the Streams instance, if its not already started
    connection = streamsx.rest.StreamingAnalyticsConnection()
    service = connection.get_streaming_analytics()
    result = service.start_instance()
    print(str(result))

def stop_streams_cloud_instance():
    print ("STOP Streaming Analytics service instance ...")
    # stop the Streams instance, if its not already stopped
    connection = streamsx.rest.StreamingAnalyticsConnection()
    service = connection.get_streaming_analytics()
    result = service.stop_instance()
    print(str(result))

def verify_compile_time_error(app, msgID):
    print ("Build sample app: "+app)
    stdout, stderr, err = run_shell_command_line('cd feature/compile.test; make '+app)
    found = False
    if (msgID in stdout or msgID in stderr):
        found = True
        print ("Message ID found: "+msgID)
    else:
        print (stdout)
        print (stderr)
    assert (found), "Error message not found: "+msgID

def generate_large_text_file(file_name):
    n = 1024 ** 2
    chars = ''.join(['x' for i in range(n)])
    with open(file_name, 'w') as f:
        for i in range(100):
            f.write(chars)
            f.write('\n')

def generate_large_bin_file(file_name):
    with open(file_name, 'wb') as fout:
       fout.write(os.urandom(1024*1024*100))

def create_app_config():
    print ("Create cos application configuration")
    stdout, stderr, err = run_shell_command_line('cd feature/param.test; make configure-json')


