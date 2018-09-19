# File Download Demo

## Description
The **FileWriteSample** application demonstrates how the 
toolkit operators can be used for objects download
from COS to the local file system. The sample consists
of two composites:
 - `ObjectStorageDataGen`: permanently creates new objects in COS.
 - `ObjectStorageScanner`: listens for the new objects in COS and loads them to the local file system as files.

## Utilized Toolkits
 - com.ibm.streamsx.objectstorage
