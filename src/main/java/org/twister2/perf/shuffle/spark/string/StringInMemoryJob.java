package org.twister2.perf.shuffle.spark.string;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.twister2.perf.shuffle.Context;
import org.twister2.perf.shuffle.io.EmptyOutputFormat;

public class StringInMemoryJob {
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("terasort");
    Configuration configuration = new Configuration();

    double size = Double.parseDouble(args[0]);
    int keySize = Integer.parseInt(args[2]);
    int dataSize = Integer.parseInt(args[3]);
    int tuples = (int) (size * 1024 * 1024 * 1024 / (keySize + dataSize));
    int parallel = Integer.parseInt(args[1]);

    configuration.set(Context.ARG_TUPLES, tuples + "");
    configuration.set(Context.ARG_PARALLEL, args[1]);
    configuration.set(Context.ARG_KEY_SIZE, args[2]);
    configuration.set(Context.ARG_DATA_SIZE, args[3]);

    inMemoryShuffle(configuration, conf, parallel);
  }

  private static void inMemoryShuffle(Configuration configuration, SparkConf conf, int parallel) {
    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaPairRDD<String, String> input = sc.newAPIHadoopRDD(configuration, StringInputFormat.class, String.class, String.class);
    JavaPairRDD<String, String> sorted = input.repartitionAndSortWithinPartitions(new HashPartitioner(parallel));
    sorted.saveAsHadoopFile("out", String.class, String.class, EmptyOutputFormat.class);
    sc.stop();
  }
}
