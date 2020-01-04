package org.twister2.perf.shuffle.spark.bigint;

import org.twister2.perf.shuffle.io.BaseRecordReader;

import java.io.IOException;
import java.math.BigInteger;

public class BigIntRecordReader extends BaseRecordReader<BigInteger, Long> {
  private BigInteger start;
  private long rand;

  public BigIntRecordReader() {
    rand = (long) (Math.random() * 100 + 1);
    start = new BigInteger("100000000000000000").multiply(new BigInteger("" + rand));
  }

  @Override
  public BigInteger getCurrentKey() throws IOException, InterruptedException {
    BigInteger i = start.add(new BigInteger("1"));
    start = i;
    return i;
  }

  @Override
  public Long getCurrentValue() throws IOException, InterruptedException {
    return rand;
  }
}
