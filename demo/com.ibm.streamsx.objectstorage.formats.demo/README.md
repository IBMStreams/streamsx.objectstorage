# Formats Demo

## Description

Application writes objects to COS in different formats (JSON, CSV, AVRO, PARQUET and PARQUET with snappy compression).

Depending on the tuple data (amount of random data generated), parquet and snappy can decrease the object size.

### Tuple with huge random parts

avro:           20676292
csv:            20803065
json:           21490578
parquet:        20541973
snappy.parquet: 20417655

### Tuple with very less random parts

avro:           20676391
csv:            20803057
json:           21490831
parquet:          503937
snappy.parquet:   376133

### Tuple with medium random parts

avro:           20676488
csv:            20803244
json:           21490889
parquet:        20541973
snappy.parquet:  7515931


## Utilized Toolkits
 - com.ibm.streamsx.objectstorage
 - com.ibm.streamsx.json
 - com.ibm.streamsx.avro
