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

1. Create a Streaming Analytics service [Streaming Analytics](https://cloud.ibm.com/docs/StreamingAnalytics).
2. Click Launch in the Streaming Analytics dashboard to launch the Streams console.
3. From the Streams console, select “Submit Job” under the “play” icon.

![Import](/streamsx.objectstorage/doc/images/streams-submit-job.png)

* Follow the [instructions](/streamsx.objectstorage/docs/user/buildsample) to build the toolkit and the sample.
* Upload the application bundle file `com.ibm.streamsx.objectstorage.sample.FunctionsSample.sab` from your file system (`com.ibm.streamsx.objectstorage.sample.FunctionsSample/output` directory). 

4. Enter the submission parameters `os-access-key-id`, `os-secret-access-key` and `os-bucket` and submit.

5. After the operators start up, they will show a green circle in the Streams graph view. If not, resubmit and verify your submission time values.

In the Streaming Analytics service, go to the Log Viewer and Click on the PE's Console Log to view output.


