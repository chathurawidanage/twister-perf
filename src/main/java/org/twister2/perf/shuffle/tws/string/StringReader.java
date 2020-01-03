package org.twister2.perf.shuffle.tws.string;

import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.config.Config;
import org.twister2.perf.io.BaseStreamInputReader;

public class StringReader extends BaseStreamInputReader<String, String> {
  public StringReader(String fileName, Config config) {
    super(fileName, config);
  }

  @Override
  public Tuple<String, String> nextRecord() {
    String[] a = currentSize.split(",");
    return new Tuple<>(a[0], a[1]);
  }
}
