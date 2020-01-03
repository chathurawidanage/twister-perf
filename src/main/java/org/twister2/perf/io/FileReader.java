package org.twister2.perf.io;

import edu.iu.dsc.tws.api.comms.structs.Tuple;

import java.io.IOException;

public interface FileReader<K, V> {
  boolean reachedEnd() throws IOException;
  Tuple<K, V> nextRecord();
}
