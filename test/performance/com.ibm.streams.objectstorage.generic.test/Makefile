# begin_generated_IBM_copyright_prolog                             
#                                                                  
# This is an automatically generated copyright prolog.             
# After initializing,  DO NOT MODIFY OR MOVE                       
# **************************************************************** 
# THIS SAMPLE CODE IS PROVIDED ON AN "AS IS" BASIS. IBM MAKES NO   
# REPRESENTATIONS OR WARRANTIES, EXPRESS OR IMPLIED, CONCERNING    
# USE OF THE SAMPLE CODE, OR THE COMPLETENESS OR ACCURACY OF THE   
# SAMPLE CODE. IBM DOES NOT WARRANT UNINTERRUPTED OR ERROR-FREE    
# OPERATION OF THIS SAMPLE CODE. IBM IS NOT RESPONSIBLE FOR THE    
# RESULTS OBTAINED FROM THE USE OF THE SAMPLE CODE OR ANY PORTION  
# OF THIS SAMPLE CODE.                                             
#                                                                  
# LIMITATION OF LIABILITY. IN NO EVENT WILL IBM BE LIABLE TO ANY   
# PARTY FOR ANY DIRECT, INDIRECT, SPECIAL OR OTHER CONSEQUENTIAL   
# DAMAGES FOR ANY USE OF THIS SAMPLE CODE, THE USE OF CODE FROM    
# THIS [ SAMPLE PACKAGE,] INCLUDING, WITHOUT LIMITATION, ANY LOST  
# PROFITS, BUSINESS INTERRUPTION, LOSS OF PROGRAMS OR OTHER DATA   
# ON YOUR INFORMATION HANDLING SYSTEM OR OTHERWISE.                
#                                                                  
# (C) Copyright IBM Corp. 2010, 2015  All Rights reserved.         
#                                                                  
# end_generated_IBM_copyright_prolog                               
.PHONY: all clean distributed javacompile

SPLC = $(STREAMS_INSTALL)/bin/sc
SPLC_FLAGS = -a -t ../../../com.ibm.streamsx.objectstorage
SPL_MAIN_COMPOSITE_NAMESPACE = com.ibm.streamsx.objectstorage.generic.perftest

SPL_CMD_ARGS ?= 

BIN_LOC=impl/java/bin
JSI = $(STREAMS_INSTALL)

all: distributed

optDir:
	mkdir -p opt

testjar: optDir
	cd ../../java/com.ibm.streamsx.objectstorage.test; ant jar
	cp -f ../../java/com.ibm.streamsx.objectstorage.test/streamsx.objectstorage.test.jar opt
	cp -f ../../java/com.ibm.streamsx.objectstorage.test/opt/downloads/*.jar opt
	cp -f ../../java/com.ibm.streamsx.objectstorage.test/opt/stocator-1.0.13-IBM-SDK-jar-with-dependencies.jar opt

toolkitjar: optDir
	cp -f ../../../com.ibm.streamsx.objectstorage/impl/lib/com.ibm.streamsx.objectstorage.jar opt

javacompile: testjar $(BIN_LOC) $(BIN_LOC)/com/ibm/streamsx/objectstorage/generic/perftest/EmptyJavaOp.class $(BIN_LOC)/com/ibm/streamsx/objectstorage/generic/perftest/OSSinkWritePerfTests.class

$(BIN_LOC)/com/ibm/streamsx/objectstorage/generic/perftest/EmptyJavaOp.class: impl/java/src/com/ibm/streamsx/objectstorage/generic/perftest/EmptyJavaOp.java
	javac -g -classpath $(JSI)/lib/com.ibm.streams.operator.jar -d $(BIN_LOC) impl/java/src/com/ibm/streamsx/objectstorage/generic/perftest/EmptyJavaOp.java

$(BIN_LOC)/com/ibm/streamsx/objectstorage/generic/perftest/OSSinkWritePerfTests.class: impl/java/src/com/ibm/streamsx/objectstorage/generic/perftest/OSSinkWritePerfTests.java
	javac -g -classpath $(JSI)/lib/com.ibm.streams.operator.jar:../../java/com.ibm.streamsx.objectstorage.test/streamsx.objectstorage.test.jar -d $(BIN_LOC) impl/java/src/com/ibm/streamsx/objectstorage/generic/perftest/OSSinkWritePerfTests.java


$(BIN_LOC):
	mkdir -p $(BIN_LOC)

distributed: toolkitjar javacompile EmptyJavaOpTest FormatEmptyJavaTest FormatTest ObjectStorageSinkCSVTest1 ObjectStorageSinkCSVTest2 ObjectStorageSinkCSVTest3 ObjectStorageSinkParquetTest1 ObjectStorageSinkParquetTest2 ObjectStorageSinkParquetTest3 ObjectStorageSinkWriteTest

EmptyJavaOpTest:
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE_NAMESPACE)::EmptyJavaOpTest $(SPL_CMD_ARGS) --output-directory=output/EmptyJavaOpTest

FormatEmptyJavaTest:
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE_NAMESPACE)::FormatEmptyJavaTest $(SPL_CMD_ARGS) --output-directory=output/FormatEmptyJavaTest

FormatTest:
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE_NAMESPACE)::FormatTest $(SPL_CMD_ARGS) --output-directory=output/FormatTest

ObjectStorageSinkCSVTest1:
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE_NAMESPACE)::ObjectStorageSinkCSVTest1 $(SPL_CMD_ARGS) --output-directory=output/ObjectStorageSinkCSVTest1

ObjectStorageSinkCSVTest2:
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE_NAMESPACE)::ObjectStorageSinkCSVTest2 $(SPL_CMD_ARGS) --output-directory=output/ObjectStorageSinkCSVTest2

ObjectStorageSinkCSVTest3:
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE_NAMESPACE)::ObjectStorageSinkCSVTest3 $(SPL_CMD_ARGS) --output-directory=output/ObjectStorageSinkCSVTest3

ObjectStorageSinkParquetTest1:
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE_NAMESPACE)::ObjectStorageSinkParquetTest1 $(SPL_CMD_ARGS) --output-directory=output/ObjectStorageSinkParquetTest1

ObjectStorageSinkParquetTest2:
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE_NAMESPACE)::ObjectStorageSinkParquetTest2 $(SPL_CMD_ARGS) --output-directory=output/ObjectStorageSinkParquetTest2

ObjectStorageSinkParquetTest3:
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE_NAMESPACE)::ObjectStorageSinkParquetTest3 $(SPL_CMD_ARGS) --output-directory=output/ObjectStorageSinkParquetTest3

ObjectStorageSinkWriteTest:
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE_NAMESPACE)::ObjectStorageSinkWriteTest $(SPL_CMD_ARGS) --output-directory=output/ObjectStorageSinkWriteTest

clean: 
	rm -rf output
	rm -rf $(BIN_LOC)
	rm -rf opt

cleangen: 
	rm -f toolkit.xml
	rm -f com.ibm.streamsx.objectstorage.generic.perftest/EmptyJavaOp/EmptyJavaOp.xml
	rm -f com.ibm.streamsx.objectstorage.generic.perftest/OSSinkWritePerfTests/OSSinkWritePerfTests.xml

