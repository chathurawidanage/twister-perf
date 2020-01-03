package org.twister2.perf.shuffle.tws.bigint;

import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.config.Config;
import org.twister2.perf.io.BaseStreamInputReader;

import java.math.BigInteger;

public class BigIntReader extends BaseStreamInputReader<BigInteger, Long> {
  public BigIntReader(String fileName, Config config) {
    super(fileName, config);
  }

  @Override
  public Tuple<BigInteger, Long> nextRecord() {
    String[] a = currentSize.split(",");
    return new Tuple<>(new BigInteger(a[0]), Long.parseLong(a[1]));
  }
}
