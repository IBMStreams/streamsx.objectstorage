.PHONY: all distributed clean 

OUTPUT_DIR = output
SPLC_FLAGS = -t $(STREAMS_INSTALL)/toolkits/com.ibm.streamsx.messagehub:$(STREAMS_INSTALL)/toolkits/com.ibm.streamsx.json
SPLC = $(STREAMS_INSTALL)/bin/sc
SPL_CMD_ARGS ?= 
SPL_MAIN_COMPOSITE = com.ibm.streamsx.datahistorian.generate.json::Main

all: distributed

distributed:
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE) $(SPL_CMD_ARGS)

clean:
	$(SPLC) $(SPLC_FLAGS) -C -M $(SPL_MAIN_COMPOSITE)
	rm -rf $(OUTPUT_DIR)
	rm -rf doc

tkidx:
	$(STREAMS_INSTALL)/bin/spl-make-toolkit -i .

