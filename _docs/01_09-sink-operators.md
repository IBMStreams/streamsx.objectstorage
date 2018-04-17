---
title: "ObjectStorageScan and S3ObjectStorageScan Operators Overview"
permalink: /docs/user/sinkoperatorsoverview/
excerpt: "Describes the usage of the Scan operators."
last_modified_at: 2018-01-10T12:37:48+01:00
redirect_from:
   - /theme-setup/
sidebar:
   nav: "userdocs"
---
{% include toc %}
{%include editme %}

### Operators Description

### Object Storage - Supported Authentication Schemes


### Supported SPL Types

The operator supports the following SPL types for the key and message attributes:

 * rstring
 * int32/int64
 * uint32/uint64
 * float32/float64
 * blob


### Parameters

| Parameter Name | Default | Description |
| --- | --- | --- |
| closeOnPunct | | Close object on punctuation. |


### Consistent Region Support

The operator can be in a consistent region and can be the start of a consistent region. The operator behaves as follows during the consistent region stages: 

#### Drain
The operator stops polling for new messages. Any records stored in the operator's buffer will be submitted until the buffer is empty. 

#### Checkpoint
The operator will save the last offset position that the KafkaConsumer client retrieved messages from. During reset, the operator will consume records starting from this offset position. 

#### Reset
The operator will seek to the offset position saved in the checkpoint. The operator will begin consuming records starting from this position. 

#### ResetToInitialState
The first time the operator was started, the initial offset that the KafkaConsumer client would begin reading from was stored in the JCP operator. When `resetToInitialState()` is called, the operator will retrieve this initial offset from the JCP and seek to this position. The operator will begin consumer records starting from this position. 

### Input Ports

The operator does not have any input ports.

### Output Ports

The operator will have a single output port. Each individual record retrieved from each of the Kafka topics will be submitted as a tuple to this output port. The `outputKeyAttributeName`, `outputMessageAttributeName` and `outputTopicAttributeName` parameters are used to specify the attributes that will contain the record contents.

### Error Handling
