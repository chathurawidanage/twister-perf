package org.twister2.perf.shuffle.spark.string;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.twister2.perf.shuffle.io.BaseInputFormat;

import java.io.IOException;

public class StringInputFormat extends BaseInputFormat<String, String> {
  @Override
  public RecordReader<String, String> createRecordReader(InputSplit inputSplit,
                                                         TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
    return new StringRecordReader();
  }
}
