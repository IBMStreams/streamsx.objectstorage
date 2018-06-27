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
  # Set the STREAMSX_OBJECTSTORAGE_TOOLKIT variable if streamsx.objectstorage sample or toolkit location changed in your environment.
  ifeq ($(STREAMSX_OBJECTSTORAGE_TOOLKIT),)
    ifneq ($(realpath ../../../com.ibm.streamsx.objectstorage),)
      STREAMSX_OBJECTSTORAGE_TOOLKIT = ../../../com.ibm.streamsx.objectstorage
    endif
    ifeq ($(STREAMSX_OBJECTSTORAGE_TOOLKIT),)
      ifneq ($(realpath $(STREAMS_INSTALL)/toolkits/com.ibm.streamsx.objectstorage),)
        STREAMSX_OBJECTSTORAGE_TOOLKIT = $(STREAMS_INSTALL)/toolkits/com.ibm.streamsx.objectstorage
      endif
    endif
  endif
  SPLC = $(STREAMS_INSTALL)/bin/sc
  SPL_PATH = $(STREAMSX_OBJECTSTORAGE_TOOLKIT)
endif

SPLC_FLAGS += -t $(SPL_PATH) --rebuild-toolkits

SPL_CMD_ARGS ?= 

build: distributed 

all: clean build

distributed: ObjectStorageScan_checkpoint_operatorDriven ObjectStorageScan_checkpoint_periodic ObjectStorageSource_checkpoint_operatorDriven ObjectStorageSource_checkpoint_periodic ObjectStorageSink_checkpoint_operatorDriven ObjectStorageSink_checkpoint_periodic ObjectStorageScan_consistent_region_unsupported_configuration ObjectStorageSource_consistent_region_unsupported_configuration ObjectStorageSink_consistent_region_unsupported_configuration ObjectStorageSource_missing_input_port_or_param ObjectStorageScan_invalid_output_port_attribute ObjectStorageSink_invalid_output_port_attribute


ObjectStorageScan_checkpoint_operatorDriven:
	-$(SPLC) $(SPLC_FLAGS) -M com.ibm.streamsx.objectstorage.test::ObjectStorageScan_checkpoint_operatorDriven --output-directory=$(OUTPUT_DIR)/ObjectStorageScan_checkpoint_operatorDriven $(SPL_CMD_ARGS)

ObjectStorageScan_checkpoint_periodic:
	-$(SPLC) $(SPLC_FLAGS) -M com.ibm.streamsx.objectstorage.test::ObjectStorageScan_checkpoint_periodic --output-directory=$(OUTPUT_DIR)/ObjectStorageScan_checkpoint_periodic $(SPL_CMD_ARGS)

ObjectStorageSource_checkpoint_operatorDriven:
	-$(SPLC) $(SPLC_FLAGS) -M com.ibm.streamsx.objectstorage.test::ObjectStorageSource_checkpoint_operatorDriven --output-directory=$(OUTPUT_DIR)/ObjectStorageSource_checkpoint_operatorDriven $(SPL_CMD_ARGS)

ObjectStorageSource_checkpoint_periodic:
	-$(SPLC) $(SPLC_FLAGS) -M com.ibm.streamsx.objectstorage.test::ObjectStorageSource_checkpoint_periodic --output-directory=$(OUTPUT_DIR)/ObjectStorageSource_checkpoint_periodic $(SPL_CMD_ARGS)

ObjectStorageSink_checkpoint_operatorDriven:
	-$(SPLC) $(SPLC_FLAGS) -M com.ibm.streamsx.objectstorage.test::ObjectStorageSink_checkpoint_operatorDriven --output-directory=$(OUTPUT_DIR)/ObjectStorageSink_checkpoint_operatorDriven $(SPL_CMD_ARGS)

ObjectStorageSink_checkpoint_periodic:
	-$(SPLC) $(SPLC_FLAGS) -M com.ibm.streamsx.objectstorage.test::ObjectStorageSink_checkpoint_periodic --output-directory=$(OUTPUT_DIR)/ObjectStorageSink_checkpoint_periodic $(SPL_CMD_ARGS)

ObjectStorageScan_consistent_region_unsupported_configuration:
	-$(SPLC) $(SPLC_FLAGS) -M com.ibm.streamsx.objectstorage.test::ObjectStorageScan_consistent_region_unsupported_configuration --output-directory=$(OUTPUT_DIR)/ObjectStorageScan_consistent_region_unsupported_configuration $(SPL_CMD_ARGS)

ObjectStorageSource_consistent_region_unsupported_configuration:
	-$(SPLC) $(SPLC_FLAGS) -M com.ibm.streamsx.objectstorage.test::ObjectStorageSource_consistent_region_unsupported_configuration --output-directory=$(OUTPUT_DIR)/ObjectStorageSource_consistent_region_unsupported_configuration $(SPL_CMD_ARGS)

ObjectStorageSink_consistent_region_unsupported_configuration:
	-$(SPLC) $(SPLC_FLAGS) -M com.ibm.streamsx.objectstorage.test::ObjectStorageSink_consistent_region_unsupported_configuration --output-directory=$(OUTPUT_DIR)/ObjectStorageSink_consistent_region_unsupported_configuration $(SPL_CMD_ARGS)

ObjectStorageSource_missing_input_port_or_param:
	-$(SPLC) $(SPLC_FLAGS) -M com.ibm.streamsx.objectstorage.test::ObjectStorageSource_missing_input_port_or_param --output-directory=$(OUTPUT_DIR)/ObjectStorageSource_missing_input_port_or_param $(SPL_CMD_ARGS)

ObjectStorageScan_invalid_output_port_attribute:
	-$(SPLC) $(SPLC_FLAGS) -M com.ibm.streamsx.objectstorage.test::ObjectStorageScan_invalid_output_port_attribute --output-directory=$(OUTPUT_DIR)/ObjectStorageScan_invalid_output_port_attribute $(SPL_CMD_ARGS)

ObjectStorageSink_invalid_output_port_attribute:
	-$(SPLC) $(SPLC_FLAGS) -M com.ibm.streamsx.objectstorage.test::ObjectStorageSink_invalid_output_port_attribute --output-directory=$(OUTPUT_DIR)/ObjectStorageSink_invalid_output_port_attribute $(SPL_CMD_ARGS)

clean:
	rm -f toolkit.xml
	rm -rf $(OUTPUT_DIR)

tkidx:
	$(STREAMS_INSTALL)/bin/spl-make-toolkit -i .

