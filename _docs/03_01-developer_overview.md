---
title: "Toolkit Development overview"
permalink: /docs/developer/overview/
excerpt: "Contributing to this toolkits development."
last_modified_at: 2017-08-04T12:37:48-04:00
redirect_from:
   - /theme-setup/
sidebar:
   nav: "developerdocs"
---
{% include toc %}
{% include editme %}


# Prerequisite

This toolkit uses Apache Ant 1.8 (or later) to build.

Internally Apache Maven 3.2 (or later) and Make are used.

Download and setup directions for Apache Maven can be found here: http://maven.apache.org/download.cgi#Installation

Set environment variable M2_HOME to the path of maven home directory.

    export M2_HOME=/opt/apache-maven-3.5.0
    export PATH=$PATH:$M2_HOME/bin

# Build the toolkit

Run the following command in the `streamsx.objectstorage` directory:

    ant all

# Build the samples

Run the following command in the `streamsx.objectstorage` directory to build all projects in the `samples` directory:

    ant build-all-samples


# Toolkit Architecture Overview

The following class diagram represents main toolkit classes and packages:

![Import](/streamsx.objectstorage/doc/images/OSToolkitHighLevelClassDiagram.png)

## Scan Operators

## Source Operators

## Sink Operators
![Import](/streamsx.objectstorage/doc/images/OSSinkOperatorArchitecture.png)

# Toolkit Java-based Tests
