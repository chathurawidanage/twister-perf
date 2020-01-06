package org.twister2.perf.shuffle.spark.bigint;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.twister2.perf.shuffle.Context;
import org.twister2.perf.shuffle.io.EmptyOutputFormat;

import java.math.BigInteger;

public class BigIntMemoryJob {
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("terasort");
    Configuration configuration = new Configuration();

    double size = Double.parseDouble(args[0]);
    int tuples = (int) size;
    int parallel = Integer.parseInt(args[1]);

    configuration.set(Context.ARG_TUPLES, tuples + "");
    configuration.set(Context.ARG_PARALLEL, args[1]);
    configuration.set(Context.ARG_KEY_SIZE, args[2]);
    configuration.set(Context.ARG_DATA_SIZE, args[3]);

    inMemoryShuffle(configuration, conf, parallel);
  }

  private static void inMemoryShuffle(Configuration configuration, SparkConf conf, int parallel) {
    JavaSparkContext sc = new JavaSparkContext(conf);
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    conf.registerKryoClasses(new Class[]{BigInteger.class, Long.class});
    JavaPairRDD<BigInteger, Long> input = sc.newAPIHadoopRDD(configuration, BigIntInputFormat.class, BigInteger.class, Long.class);
    JavaPairRDD<BigInteger, Long> sorted = input.repartitionAndSortWithinPartitions(new HashPartitioner(parallel));
    sorted.saveAsHadoopFile("out", BigInteger.class, Long.class, EmptyOutputFormat.class);
    sc.stop();
  }
}
