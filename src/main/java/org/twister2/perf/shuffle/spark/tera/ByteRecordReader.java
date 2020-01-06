package org.twister2.perf.shuffle.spark.tera;

import org.twister2.perf.shuffle.io.BaseRecordReader;

import java.io.IOException;
import java.util.Random;
import java.util.logging.Logger;

public class ByteRecordReader extends BaseRecordReader<byte[], byte[]> {
  private static final Logger LOG = Logger.getLogger(ByteRecordReader.class.getName());

  private Random random;

  public ByteRecordReader() {
    random = new Random(System.nanoTime());
  }

  @Override
  public byte[] getCurrentKey() throws IOException, InterruptedException {
    byte[] key = new byte[keySize];
    random.nextBytes(key);
    return key;
  }

  @Override
  public byte[] getCurrentValue() throws IOException, InterruptedException {
    byte[] key = new byte[dataSize];
    random.nextBytes(key);
    return key;
  }
}
