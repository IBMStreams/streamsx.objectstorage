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
