package org.twister2.perf.shuffle.io;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

public class InMemoryInputSplit extends InputSplit implements Writable {
  private int elements = 10000000;

  private String node;

  private int keySize;

  private int dataSize;

  public InMemoryInputSplit(int elements, int keySize, int dataSize, String node) {
    this.elements = elements;
    this.keySize = keySize;
    this.dataSize = dataSize;
    this.node = node;
  }

  public InMemoryInputSplit() {
  }

  @Override
  public long getLength() throws IOException, InterruptedException {
    return elements * (keySize + dataSize);
  }

  @Override
  public String[] getLocations() throws IOException, InterruptedException {
    String[] ret = new String[1];
    ret[0] = node;
    return ret;
  }

  public String getNode() {
    return node;
  }

  public void setNode(String node) {
    this.node = node;
  }

  public void setElements(int elements) {
    this.elements = elements;
  }

  public void setKeySize(int keySize) {
    this.keySize = keySize;
  }

  public void setDataSize(int dataSize) {
    this.dataSize = dataSize;
  }

  public int getElements() {
    return elements;
  }

  public int getKeySize() {
    return keySize;
  }

  public int getDataSize() {
    return dataSize;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    byte[] b = ByteBuffer.allocate(12).putInt(elements).putInt(keySize).putInt(dataSize).array();
    dataOutput.write(b);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    elements = dataInput.readInt();
    keySize = dataInput.readInt();
    dataSize = dataInput.readInt();
  }
}
