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
SPL_MAIN_COMPOSITE = com.ibm.streamsx.objectstorage.s3.test::WriteDurationTest
SPL_MAIN_COMPOSITE1 = com.ibm.streamsx.objectstorage.s3.test::WriteDurationTestIAM
SPL_MAIN_COMPOSITE2 = com.ibm.streamsx.objectstorage.s3.test::WriteDurationTestJava

BIN_LOC=impl/java/bin

build: distributed 

all: clean build

$(BIN_LOC):
	mkdir -p $(BIN_LOC)

javacompile: $(BIN_LOC) $(BIN_LOC)/com/ibm/streamsx/objectstorage/perf/test/TestSource.class $(BIN_LOC)/com/ibm/streamsx/objectstorage/perf/test/TestSink.class

$(BIN_LOC)/com/ibm/streamsx/objectstorage/perf/test/TestSink.class: impl/java/src/com/ibm/streamsx/objectstorage/perf/test/TestSink.java
	javac -g -classpath $(STREAMS_INSTALL)/lib/com.ibm.streams.operator.jar -d $(BIN_LOC) impl/java/src/com/ibm/streamsx/objectstorage/perf/test/TestSink.java

$(BIN_LOC)/com/ibm/streamsx/objectstorage/perf/test/TestSource.class: impl/java/src/com/ibm/streamsx/objectstorage/perf/test/TestSource.java
	javac -g -classpath $(STREAMS_INSTALL)/lib/com.ibm.streams.operator.jar:impl/lib/samples.jar -d $(BIN_LOC) impl/java/src/com/ibm/streamsx/objectstorage/perf/test/TestSource.java

distributed: javacompile
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE) --output-directory=$(OUTPUT_DIR) $(SPL_CMD_ARGS)
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE1) --output-directory=$(OUTPUT_DIR)/iam $(SPL_CMD_ARGS)
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE2) --output-directory=$(OUTPUT_DIR)/java $(SPL_CMD_ARGS)

clean:
	$(SPLC) $(SPLC_FLAGS) -C -M $(SPL_MAIN_COMPOSITE)
	$(SPLC) $(SPLC_FLAGS) -C -M $(SPL_MAIN_COMPOSITE1)
	rm -rf $(OUTPUT_DIR)

tkidx:
	$(STREAMS_INSTALL)/bin/spl-make-toolkit -i .

