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
  ifeq ($(STREAMSX_MESSAGEHUB_TOOLKIT),)
    ifneq ($(realpath ../../../streamsx.messagehub/com.ibm.streamsx.messagehub),)
      STREAMSX_MESSAGEHUB_TOOLKIT=../../../streamsx.messagehub/com.ibm.streamsx.messagehub
    endif
  endif
  STREAMSX_JSON_TOOLKIT ?=$(STREAMS_INSTALL)/toolkits/com.ibm.streamsx.json
  SPLC = $(STREAMS_INSTALL)/bin/sc
  ADDITIONAL_TOOLKITS=:$(STREAMSX_JSON_TOOLKIT)
  ifneq ($(STREAMSX_MESSAGEHUB_TOOLKIT),)
    ADDITIONAL_TOOLKITS:=:$(STREAMSX_MESSAGEHUB_TOOLKIT):$(STREAMSX_JSON_TOOLKIT)
  endif
  SPL_PATH=$(STREAMSX_OBJECTSTORAGE_TOOLKIT)$(ADDITIONAL_TOOLKITS)
endif

SPLC_FLAGS += -t $(SPL_PATH) --rebuild-toolkits

SPL_CMD_ARGS ?= 


SPL_MAIN_COMPOSITE1 = com.ibm.streamsx.objectstorage.sample.complex::ParquetWithPartitionSample
SPL_MAIN_COMPOSITE2 = com.ibm.streamsx.objectstorage.sample.complex::TimeRollingPolicyBasicAuthSample
SPL_MAIN_COMPOSITE3 = com.ibm.streamsx.objectstorage.sample.complex::LoadPickleSample
SPL_MAIN_COMPOSITE4 = com.ibm.streamsx.objectstorage.sample.complex::ScanComplexPatternSample
SPL_MAIN_COMPOSITE5 = com.ibm.streamsx.objectstorage.sample.complex::TimeRollingWithMHSample


build: distributed 

all: clean build

distributed:
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE1) $(SPL_CMD_ARGS) --output-directory=$(OUTPUT_DIR)/ParquetWithPartitionSample
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE2) $(SPL_CMD_ARGS) --output-directory=$(OUTPUT_DIR)/TimeRollingPolicySample
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE3) $(SPL_CMD_ARGS) --output-directory=$(OUTPUT_DIR)/LoadPickleSample
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE4) $(SPL_CMD_ARGS) --output-directory=$(OUTPUT_DIR)/ScanComplexPatternSample
	#$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE5) $(SPL_CMD_ARGS) --output-directory=$(OUTPUT_DIR)/TimeRollingWithMHSample
	
clean:	
	rm -rf $(OUTPUT_DIR)
	rm -rf doc

