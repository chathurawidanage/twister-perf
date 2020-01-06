package org.twister2.perf.shuffle.io;

import org.apache.hadoop.mapreduce.*;
import org.twister2.perf.shuffle.Context;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public abstract class BaseInputFormat<K, V> extends InputFormat<K, V> {
  private static final Logger LOG = Logger.getLogger(BaseInputFormat.class.getName());
  @Override
  public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
    int parallel = jobContext.getConfiguration().getInt(Context.ARG_PARALLEL, 16);
    int keySize = jobContext.getConfiguration().getInt(Context.ARG_KEY_SIZE, 10);
    int dataSize = jobContext.getConfiguration().getInt(Context.ARG_DATA_SIZE, 90);
    int elements = jobContext.getConfiguration().getInt(Context.ARG_TUPLES, 10000);

    LOG.info(String.format("Format configuration parallel %d, key %d, data %d, tuples %d",
        parallel, keySize, dataSize, elements));

    List<InputSplit> splits = new ArrayList<>();
    for (int i = 0; i < parallel; i++) {
      String node = "d-00";
      int index = i % 16;
      if (index >= 10) {
        node += index;
      } else {
        node += "0" + index;
      }
      InMemoryInputSplit e = new InMemoryInputSplit(elements, keySize, dataSize, node);
      e.setNode(node);
      splits.add(e);
    }
    return splits;
  }

}
