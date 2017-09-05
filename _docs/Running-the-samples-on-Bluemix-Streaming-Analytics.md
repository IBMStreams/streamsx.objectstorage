1. Create a Streaming Analytics service by following the “Finding the service” section of [Introduction to Bluemix Streaming Analytics](https://developer.ibm.com/streamsdev/docs/streaming-analytics-now-available-bluemix/).
2. Click Launch in the Streaming Analytics Bluemix dashboard to launch the Streams console.
3. From the Streams console, select “Submit Job” under the “play” icon.

   [[images/streams-submit-job.png]]

* Follow the [instructions](https://github.com/IBMStreams/streamsx.objectstorage/wiki/Build-the-toolkit-and-the-samples) to build the toolkit and the sample.
* Upload the application bundle file `com.ibm.streamsx.objectstorage.swift.sample.Main.sab` from your file system (`com.ibm.streamsx.objectstorage.swift.sample/output` directory). 

4. Enter the submission parameters `ObjectStorage-ProjectId`, `ObjectStorage-UserId` and `ObjectStorage-Password` with the values from the Service Credentials and submit.

[[images/Submit.png]]

5. After the operators start up, they will show a green circle in the Streams graph view. If not, resubmit and verify your submission time values.

In the Streaming Analytics service, go to the Log Viewer and Click on the PE's Console Log to view output.

[[images/logviewer.png]]