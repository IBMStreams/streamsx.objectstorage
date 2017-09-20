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
  # Set this variable if streamsx.metrics sample or toolkit location changed in your environment.
  STREAMSX_OBJECTSTORAGE_TOOLKIT ?=../../com.ibm.streamsx.objectstorage
  SPLC = $(STREAMS_INSTALL)/bin/sc
  SPL_PATH = $(STREAMSX_OBJECTSTORAGE_TOOLKIT)
endif

SPLC_FLAGS += -t $(SPL_PATH) --output-directory=$(OUTPUT_DIR) --rebuild-toolkits

SPL_CMD_ARGS ?= 
SPL_MAIN_COMPOSITE1 = com.ibm.streamsx.objectstorage.sample::ObjectReadWriteSample
SPL_MAIN_COMPOSITE2 = com.ibm.streamsx.objectstorage.sample::ScanSample
SPL_MAIN_COMPOSITE3 = com.ibm.streamsx.objectstorage.sample::TimeRollingPolicySample
SPL_MAIN_COMPOSITE4 = com.ibm.streamsx.objectstorage.sample::LoadPickleSample
SPL_MAIN_COMPOSITE5 = com.ibm.streamsx.objectstorage.sample::ScanComplexPatternSample

build: distributed 

all: clean build

distributed:
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE1) $(SPL_CMD_ARGS)
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE2) $(SPL_CMD_ARGS)
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE3) $(SPL_CMD_ARGS)
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE4) $(SPL_CMD_ARGS)
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE5) $(SPL_CMD_ARGS)
	
clean:	
	rm -rf $(OUTPUT_DIR)

