# Licensed Materials - Property of IBM
# ObjectStorage Toolkit for IBM Streams
# (c) Copyright IBM Corp. 2018
# All rights reserved.                              
.PHONY: all clean

all: 
	for dir in $(shell ls -d -1 */); do\
		make -C $$dir || exit 1; \
	done

clean:
	for dir in $(shell ls -d -1 */); do\
		make -C $$dir clean || exit 1; \
	done

tkidx:
	for dir in $(shell ls -d -1 */); do\
		make -C $$dir tkidx || exit 1; \
	done

make-doc:
	for dir in $(shell ls -d -1 */); do echo -e "\n######## make-doc sample $$dir ########"; cd $$dir; spl-make-doc -i . --include-all; cd -; done


