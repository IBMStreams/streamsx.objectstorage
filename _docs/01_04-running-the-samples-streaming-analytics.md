---
title: "Running the samples"
permalink: /docs/user/runsample
excerpt: "How to use this toolkit."
last_modified_at: 2017-08-04T12:37:48-04:00
redirect_from:
   - /theme-setup/
sidebar:
   nav: "userdocs"
---
{% include toc %}
{%include editme %}

1. Create a Streaming Analytics service by following the “Finding the service” section of [Introduction to Bluemix Streaming Analytics](https://developer.ibm.com/streamsdev/docs/streaming-analytics-now-available-bluemix/).
2. Click Launch in the Streaming Analytics Bluemix dashboard to launch the Streams console.
3. From the Streams console, select “Submit Job” under the “play” icon.

![Import](/streamsx.objectstorage/doc/images/streams-submit-job.png)

* Follow the [instructions](/docs/user/buildsample) to build the toolkit and the sample.
* Upload the application bundle file `com.ibm.streamsx.objectstorage.swift.sample.Main.sab` from your file system (`com.ibm.streamsx.objectstorage.swift.sample/output` directory). 

4. Enter the submission parameters `ObjectStorage-ProjectId`, `ObjectStorage-UserId` and `ObjectStorage-Password` with the values from the Service Credentials and submit.

![Import](/streamsx.objectstorage/doc/images/Submit.png)

5. After the operators start up, they will show a green circle in the Streams graph view. If not, resubmit and verify your submission time values.

In the Streaming Analytics service, go to the Log Viewer and Click on the PE's Console Log to view output.

![Import](/streamsx.objectstorage/doc/images/logviewer.png)
