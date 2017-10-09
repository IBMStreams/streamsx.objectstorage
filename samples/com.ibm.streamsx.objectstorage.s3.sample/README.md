## com.ibm.streamsx.objectstorage.s3.sample

This sample application shows the usage of the Operator and functions to access Object Storage.
A sample bucket is created, the sample file is written to Object Storage, then read from Object Storage.
Finally the object and the bucket is deleted.

## Use

Build the application:

`make`

Run:

In the Streaming Analytics service, click LAUNCH to open the Streams Console, where you can submit and manage your jobs.
Upload the application bundle file ./output/com.ibm.streamsx.objectstorage.s3.sample.Main.sab from your file system.

In the Streaming Analytics service, go to the Log Viewer and Click on the PE's Console Log to view output

Clean:

`make clean`


