# Copyright (C) 2018  International Business Machines Corporation
# All Rights Reserved
# start a streaming analytics service in the cloud
# Paramater (optional): --streaming_analytics_service_name (default from env STREAMING_ANALYTICS_SERVICE_NAME)
# Paramater (optional): --vcap_services (default from env VCAP_SERVICES)

import sys
import streamsx.rest
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--streaming_analytics_service_name", help="overwrites env STREAMING_ANALYTICS_SERVICE_NAME")
parser.add_argument("--vcap_services", help="overwrites env VCAP_SERVICES")
args = parser.parse_args()

# start the Streams instance, if its not already started
# uses environment variables STREAMING_ANALYTICS_SERVICE_NAME and VCAP_SERVICES to connect to IBM Cloud Service
# optional input parameter streaming_analytics_service_name overwrites env STREAMING_ANALYTICS_SERVICE_NAME
# optional input parameter vcap_services overwrites env VCAP_SERVICES
if args.streaming_analytics_service_name:
    if args.vcap_services:
       connection = streamsx.rest.StreamingAnalyticsConnection(service_name=args.streaming_analytics_service_name,vcap_services=args.vcap_services)
    else:
       connection = streamsx.rest.StreamingAnalyticsConnection(service_name=args.streaming_analytics_service_name)
else:
    if args.vcap_services:
       connection = streamsx.rest.StreamingAnalyticsConnection(vcap_services=args.vcap_services)
    else:  
       connection = streamsx.rest.StreamingAnalyticsConnection()
    
service = connection.get_streaming_analytics()
result = service.start_instance()


sys.exit(0)

