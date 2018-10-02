# Licensed Materials - Property of IBM
# ObjectStorage Toolkit for IBM Streams
# (c) Copyright IBM Corp. 2018
# All rights reserved.                              
.PHONY: all clean

all: 
	cd dh_generate_json; make all
	cd dh_json_parquet; make all

clean:
	rm -rf tk*
	cd dh_generate_json; make clean
	cd dh_json_parquet; make clean

tkidx:
	cd dh_generate_json; make tkidx
	cd dh_json_parquet; make tkidx

showenv:
	@echo "========================================"
	@echo `python --version`
	@echo JAVA_HOME=$(JAVA_HOME)
	@echo VCAP_SERVICES=$(VCAP_SERVICES)
	@echo STREAMING_ANALYTICS_SERVICE_NAME=$(STREAMING_ANALYTICS_SERVICE_NAME)
	@echo COS_URI=$(COS_URI)
	@echo COS_TOOLKIT=$(COS_TOOLKIT)
	@echo MH_TOOLKIT=$(MH_TOOLKIT)
	@echo "========================================"