# Licensed Materials - Property of IBM
# ObjectStorage Toolkit for IBM Streams
# (c) Copyright IBM Corp. 2018
# All rights reserved.
.PHONY: all clean 

SPLC_FLAGS = -a
OUTPUT_DIR = output
ifeq ($(STREAMS_STUDIO_BUILDING), 1)
  # Building from Streams Studio, use env vars set by studio
  SPLC = $(STREAMS_STUDIO_SC_PATH)
  SPL_PATH = $(STREAMS_STUDIO_SPL_PATH)
  OUTPUT_DIR = $(STREAMS_STUDIO_OUTPUT_DIRECTORY)
else
  # Building outside of Streams Studio...
  ifeq ($(STREAMS_INSTALL),)
    $(error error: environment variable STREAMS_INSTALL has to be set)
  endif
  # Set this variable if streamsx.objectstorage sample or toolkit location changed in your environment.
  STREAMSX_OBJECTSTORAGE_TOOLKIT ?=../../com.ibm.streamsx.objectstorage
  ifeq ($(STREAMSX_MESSAGEHUB_TOOLKIT),)
    ifneq ($(realpath ../../../streamsx.messagehub/com.ibm.streamsx.messagehub),)
      STREAMSX_MESSAGEHUB_TOOLKIT=../../../streamsx.messagehub/com.ibm.streamsx.messagehub
    endif
  endif
  ifeq ($(STREAMSX_JSON_TOOLKIT),)
    ifneq ($(realpath ../../../streamsx.json/com.ibm.streamsx.json),)
      STREAMSX_JSON_TOOLKIT=../../../streamsx.json/com.ibm.streamsx.json
    endif
  endif
  ifeq ($(STREAMSX_JSON_TOOLKIT),)  
     STREAMSX_JSON_TOOLKIT ?=$(STREAMS_INSTALL)/toolkits/com.ibm.streamsx.json
  endif
  ifeq ($(STREAMSX_MESSAGEHUB_TOOLKIT),)  
     STREAMSX_MESSAGEHUB_TOOLKIT ?=$(STREAMS_INSTALL)/toolkits/com.ibm.streamsx.messagehub
  endif
  ADDITIONAL_TOOLKITS:=:$(STREAMSX_MESSAGEHUB_TOOLKIT):$(STREAMSX_JSON_TOOLKIT)
  SPL_PATH=$(STREAMSX_OBJECTSTORAGE_TOOLKIT)$(ADDITIONAL_TOOLKITS)
  SPLC = $(STREAMS_INSTALL)/bin/sc
endif

SPLC_FLAGS += -t $(SPL_PATH) --rebuild-toolkits
SPL_CMD_ARGS ?= 
SPL_MAIN_COMPOSITE1=com.ibm.streamsx.objectstorage.messagehub.sample::MH2ObjectStorageCSVSample
SPL_MAIN_COMPOSITE2=com.ibm.streamsx.objectstorage.messagehub.sample::MH2ObjectStoragePartitionedParquetSample

all: distributed

distributed:
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE1) --output-directory=$(OUTPUT_DIR)/csv $(SPL_CMD_ARGS)
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE2) --output-directory=$(OUTPUT_DIR)/parquet $(SPL_CMD_ARGS)

clean:
	$(SPLC) $(SPLC_FLAGS) -C -M $(SPL_MAIN_COMPOSITE1)
	$(SPLC) $(SPLC_FLAGS) -C -M $(SPL_MAIN_COMPOSITE2)
	rm -fr output impl
	rm -fr toolkit.xml

tkidx:
	$(STREAMS_INSTALL)/bin/spl-make-toolkit -i .
