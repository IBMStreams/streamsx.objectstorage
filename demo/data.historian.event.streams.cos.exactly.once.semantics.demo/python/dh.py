from streamsx.topology.topology import *
from streamsx.topology.schema import CommonSchema
from streamsx.topology.context import submit
from streamsx.topology.state import ConsistentRegionConfig

import os
import uuid
import time

import streamsx.objectstorage as cos
import streamsx.messagehub as mh

class JsonData(object):
    def __init__(self, prefix, count, delay=True):
        self.prefix = prefix
        self.count = count
        self.delay = delay
    def __call__(self):
        # allow time to get the consumer started.
        if self.delay:
            time.sleep(10)  
        for i in range(self.count):
            yield {'p': self.prefix + '_' + str(i), 'c': i}


def get_credentials_cos():
    result = None
    try:
        cred_file = os.environ['COS_SERVICE_CREDENTIALS']
        with open(cred_file) as data_file:
            result = json.load(data_file)
    except KeyError: 
        result = None
    return result

def get_credentials_eventstreams():
    result = None
    try:
        cred_file = os.environ['EVENTSTREAMS_SERVICE_CREDENTIALS']
        with open(cred_file) as data_file:
            result = json.load(data_file)
    except KeyError: 
        result = None
    return result

def create_dh_demo_app():

    # configuration of parallelization
    num_partitions=2
    num_consumers=num_partitions
    num_cos_writers=2
    # configuration of consistent region trigger period
    trigger_period=60
    # Event Streams topic with num_partitions (needs to be created before launching this app)
    topic = os.environ["EVENT_STREAMS_TOPIC"]

    topo = Topology('DataHistorianSample')

    # add toolkits (requires streamsx.messagehub>=1.5.1 and streamsx.objectstorage>=1.8.0)
    streamsx.spl.toolkit.add_toolkit(topo, os.environ["MH_TOOLKIT"])
    streamsx.spl.toolkit.add_toolkit(topo, os.environ["COS_TOOLKIT"])

    # DATA GENERATOR - write n JSON messages into num_partitions Kafka partitions
    n = 6000000
    uid = str(uuid.uuid4())
    gendata = topo.source(JsonData(uid, n)).set_parallel(num_partitions)
    gendatajson = gendata.as_json()
    mh.publish(gendatajson, topic, credentials=get_credentials_eventstreams())


    # ------ INGEST start --------------------------
    # Event Streams Consumer produces JSON output parsed by map function and converted to structured stream

    # Event Streams Consumer
    js = mh.subscribe(topo, topic, CommonSchema.Json, credentials=get_credentials_eventstreams()).set_parallel(num_consumers)
    js.set_consistent(ConsistentRegionConfig.periodic(trigger_period))
    
    # JSON to tuple
    s = js.map(schema='tuple<rstring p, int32 c>')
    s.colocate(js)
    end_ingest = s.end_parallel()
    # ------ INGEST end ----------------------------


    # ------ CUSTOM BUSINESS LOGIC start -----------
    # dummy tuple forwarder
    to_cos = end_ingest.map()
    to_cos.isolate()
    # ------ CUSTOM BUSINESS LOGIC end -------------


    # COS-SINK
    bucket = os.environ["COS_BUCKET"]
    endpoint = os.environ["COS_ENDPOINT"]
    cos.write_parquet(to_cos.parallel(num_cos_writers), bucket=bucket, endpoint=endpoint, object='DataHistorian/%CHANNEL_%OBJECTNUM.parquet', vm_arg='-Xmx 8192m', credentials=get_credentials_cos())
    
    return topo


submit('STREAMING_ANALYTICS_SERVICE', create_dh_demo_app())
