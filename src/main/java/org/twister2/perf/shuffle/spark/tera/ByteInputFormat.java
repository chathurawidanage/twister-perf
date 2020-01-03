package org.twister2.perf.shuffle.spark.tera;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.twister2.perf.shuffle.io.BaseInputFormat;
import org.twister2.perf.shuffle.io.InMemoryInputSplit;

import java.io.IOException;

public class ByteInputFormat extends BaseInputFormat<byte[], byte[]> {
  @Override
  public RecordReader<byte[], byte[]> createRecordReader(InputSplit inputSplit,
                                                         TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
    if (inputSplit instanceof InMemoryInputSplit) {
      return new ByteRecordReader();
    } else {
      throw new RuntimeException("Un-expected split");
    }
  }
}
