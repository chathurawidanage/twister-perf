package org.twister2.perf.shuffle.io;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.logging.Logger;

public abstract class BaseRecordReader<K, V> extends RecordReader<K, V> {
  private static final Logger LOG = Logger.getLogger(BaseRecordReader.class.getName());

  protected int numRecords;
  protected int currentRead = 0;
  protected int keySize;
  protected int dataSize;

  public BaseRecordReader() {
  }

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
    if (inputSplit instanceof InMemoryInputSplit) {
      InMemoryInputSplit split = (InMemoryInputSplit) inputSplit;
      numRecords = split.getElements();
      keySize = split.getKeySize();
      dataSize = split.getDataSize();
      LOG.info(String.format("Records %d, key %d, data %d", numRecords, keySize, dataSize));
    } else {
      throw new IOException("Not a InMemoryInputSplit");
    }
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    return currentRead++ < numRecords;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return currentRead / numRecords;
  }

  @Override
  public void close() throws IOException {

  }
}
