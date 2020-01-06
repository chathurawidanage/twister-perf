package org.twister2.perf.shuffle.spark.string;

import org.twister2.perf.shuffle.io.BaseRecordReader;

import java.io.IOException;

public class StringRecordReader extends BaseRecordReader<String, String> {
  @Override
  public String getCurrentKey() throws IOException, InterruptedException {
    return getAlphaNumericString(keySize);
  }

  @Override
  public String getCurrentValue() throws IOException, InterruptedException {
    return getAlphaNumericString(dataSize);
  }

  // function to generate a random string of length n
  private static String getAlphaNumericString(int n) {
    // chose a Character random from this String
    String AlphaNumericString = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        + "0123456789"
        + "abcdefghijklmnopqrstuvxyz";

    // create StringBuffer size of AlphaNumericString
    StringBuilder sb = new StringBuilder(n);

    for (int i = 0; i < n; i++) {
      // generate a random number between
      // 0 to AlphaNumericString variable length
      int index
          = (int) (AlphaNumericString.length()
          * Math.random());
      // add Character one by one in end of sb
      sb.append(AlphaNumericString
          .charAt(index));
    }
    return sb.toString();
  }
}
