---
title: "Building the samples"
permalink: /docs/user/buildsample
excerpt: "How to use this toolkit."
last_modified_at: 2017-08-04T12:37:48-04:00
redirect_from:
   - /theme-setup/
sidebar:
   nav: "userdocs"
---
{% include toc %}
{%include editme %}

# Build the samples

Run the following command in the `streamsx.objectstorage` directory to build all projects in the `samples` directory:

    ant build-all-samples

## Build the sample in Streams Studio

* Add the *com.ibm.streamsx.inet* toolkit (from GitHub) and the *com.ibm.streamsx.objectstorage* toolkit to Streams Studio by following the “Procedure” section of [Adding toolkit locations](https://www.ibm.com/support/knowledgecenter/en/SSCRJU_4.2.0/com.ibm.streams.studio.doc/doc/tusing-working-with-toolkits-adding-toolkit-locations.html).
* Import a sample project, for example **com.ibm.streamsx.objectstorage.swift.sample**, into Streams Studio from the `streamsx.objectstorage/samples` directory. If the project doesn’t build successfully, make sure you have added the already built *com.ibm.streamsx.inet* toolkit (from GitHub) and the *com.ibm.streamsx.objectstorage* toolkit to Streams Studio. If you import the project with the option "Copy projects into workspace" selected, then you will find the application bundle file in the `<workspace>/com.ibm.streamsx.objectstorage.swift.sample/output` directory.

![Import](/streamsx.objectstorage/doc/images/import.png)
