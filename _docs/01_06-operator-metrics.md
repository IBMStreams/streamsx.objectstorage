---
title: "Opeators metrics"
permalink: /docs/user/operatormetrics
excerpt: "Operator metrics."
last_modified_at: 2018-03-18T12:37:48-04:00
redirect_from:
   - /theme-setup/
sidebar:
   nav: "userdocs"
---
{% include toc %}
{%include editme %}

# ObjectStorageSink

|Metric		                |Description							   |
|---------------------------|------------------------------------------|
|nActiveObjects	            | Number of active (open) objects|
|nClosedObjects	            | Number of closed objects|
|nExpiredObjects            | Number of objects expired according to rolling policy|
|nEvictedObjects            | Number of objects closed by the operator ahead of time due to memory constraints|
|nMaxConcurrentParitionsNum	| Maximum number of concurrent partitions|
|startupTimeMillisecs	    | Operator startup time in milliseconds|

