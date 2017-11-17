# Copyright (C) 2017, International Business Machines Corporation. 
# All Rights Reserved.

.PHONY: build all clean distributed

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
  SPLC = $(STREAMS_INSTALL)/bin/sc
  SPL_PATH=$(STREAMSX_OBJECTSTORAGE_TOOLKIT)
endif

SPLC_FLAGS += -t $(SPL_PATH) --rebuild-toolkits
SPL_CMD_ARGS ?= 

SPL_MAIN_COMPOSITE1 = com.ibm.streamsx.objectstorage.s3.sample::BucketScanSample
SPL_MAIN_COMPOSITE2 = com.ibm.streamsx.objectstorage.s3.sample::ReadWriteSample
SPL_MAIN_COMPOSITE3 = com.ibm.streamsx.objectstorage.s3.sample::ObjectTemplateWriteSample

build: distributed

all: clean build

distributed:
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE1) $(SPL_CMD_ARGS) --output-directory=$(OUTPUT_DIR)/BucketScanSample
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE2) $(SPL_CMD_ARGS) --output-directory=$(OUTPUT_DIR)/ReadWriteSample
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE3) $(SPL_CMD_ARGS) --output-directory=$(OUTPUT_DIR)/ObjectTemplateWriteSample

clean:
	$(SPLC) $(SPLC_FLAGS) -C -M $(SPL_MAIN_COMPOSITE1)
	rm -rf $(OUTPUT_DIR)
	rm -rf doc

