# twister-perf

Measuring performance of twister2

## Generating HDFS Data

./bin/twister2 submit standalone jar ~/twister2/delta/twister-perf/target/twister-perf-0.1.0-SNAPSHOT.jar org.twister2.perf.shuffle.tws.bigint.WritingJob  hdfs://localhost:9001/80_1 80 16000 true 50000000 1>&2 | tee out.txt

## Running Input Partition Job

time ./bin/twister2 submit standalone jar ~/twister2/delta/twister-perf/target/twister-perf-0.1.0-SNAPSHOT.jar org.twister2.perf.shuffle.tws.bigint.InputPartitionJob  hdfs://localhost:9001/80_1 80 16000 true true 1>&2 | tee out.txt
