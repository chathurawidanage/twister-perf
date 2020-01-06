package org.twister2.perf.shuffle.spark.bigint;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.twister2.perf.shuffle.io.BaseInputFormat;

import java.io.IOException;
import java.math.BigInteger;

public class BigIntInputFormat extends BaseInputFormat<BigInteger, Long> {
  @Override
  public RecordReader<BigInteger, Long> createRecordReader(InputSplit inputSplit,
                                                           TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
    return new BigIntRecordReader();
  }
}
