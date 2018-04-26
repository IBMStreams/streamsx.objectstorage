# File Write Sample

## Description
The **FileWriteSample** sample demonstrates how the 
toolkit operators can be used for objects download
from COS to the local file system. The sample consists
of two composites:
 - ObjectStorageDataGen: permanently creates new objects in COS 
 - ObjectStorageScanner: listen for the new objects in COS and upload them to the local file system.

## Utilized Toolkits
 - com.ibm.streamsx.objectstorage