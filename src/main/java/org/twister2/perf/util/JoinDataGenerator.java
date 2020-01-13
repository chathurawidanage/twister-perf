package org.twister2.perf.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class JoinDataGenerator {
  public static void main(String[] args) throws IOException {
    int recordsPerRelation = Integer.parseInt(args[0]);
    int nodes = Integer.parseInt(args[1]);
    String out = args[2];
    long noOfTuplesPerNode = recordsPerRelation / nodes;
    System.out.println("Generating " + noOfTuplesPerNode + " tuples per node...");
    Random random = new Random(System.nanoTime());
    for (int node = 0; node < nodes; node++) {
      BufferedWriter br1 = new BufferedWriter(new FileWriter(new File(out + "/in1/" + node)));
      BufferedWriter br2 = new BufferedWriter(new FileWriter(new File(out + "/in2/" + node)));
      for (int i = 0; i < noOfTuplesPerNode; i++) {
        br1.write(String.format("%d\t%d", random.nextInt(recordsPerRelation), random.nextLong()));
        br1.newLine();
        br2.write(String.format("%d\t%d", random.nextInt(recordsPerRelation), random.nextLong()));
        br2.newLine();
      }
      br1.close();
      br2.close();
    }
  }
}
