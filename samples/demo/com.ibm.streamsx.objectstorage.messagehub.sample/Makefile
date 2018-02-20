.PHONY: all clean 

# Variables passed in: SPL_MAIN_COMPOSITE TOPOLOGY_PATH
#SPLC_FLAGS = -a 

#ROOT_DIR:=$(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))
ROOT_DIR:=$(shell pwd)

SPLC_FLAGS ?= --data-directory=$(ROOT_DIR) --no-mixed-mode-preprocessing --rebuild-toolkits -a -t $(ROOT_DIR)/../../../:$(STREAMS_INSTALL)/toolkits
SPLPC_FLAGS ?= -i $(ROOT_DIR)
SPLC = $(STREAMS_INSTALL)/bin/sc
SPLM = $(STREAMS_INSTALL)/bin/spl-make-toolkit
SPL_CMD_ARGS ?=
SPL_MAIN_COMPOSITE=com.ibm.streamsx.objectstorage.messagehub.sample::MH2ObjectStorageCSVSample

all: distributed

distributed:
	$(SPLM) $(SPLPC_FLAGS) 
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE) $(SPL_CMD_ARGS) 

clean:
	$(SPLC) $(SPLC_FLAGS) -C -M $(SPL_MAIN_COMPOSITE)
	rm -fr output impl
	rm -fr info.xml toolkit.xml
