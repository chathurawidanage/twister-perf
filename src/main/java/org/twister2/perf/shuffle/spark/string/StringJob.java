package org.twister2.perf.shuffle.spark.string;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.HashPartitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.twister2.perf.shuffle.Context;
import org.twister2.perf.shuffle.io.EmptyOutputFormat;
import org.twister2.perf.shuffle.spark.tera.ByteComparator;
import org.twister2.perf.shuffle.spark.tera.ByteInputFormat;
import org.twister2.perf.shuffle.spark.tera.TeraSortPartitioner;
import scala.Tuple2;

public class StringJob {
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("terasort");
    Configuration configuration = new Configuration();

    double size = Double.parseDouble(args[0]);
    int keySize = Integer.parseInt(args[2]);
    int dataSize = Integer.parseInt(args[3]);
    int tuples = (int) (size * 1024 * 1024 * 1024 / (keySize + dataSize));
    boolean writeToFile = Boolean.parseBoolean(args[4]);
    int parallel = Integer.parseInt(args[1]);

    configuration.set(Context.ARG_TUPLES, tuples + "");
    configuration.set(Context.ARG_PARALLEL, args[1]);
    configuration.set(Context.ARG_KEY_SIZE, args[2]);
    configuration.set(Context.ARG_DATA_SIZE, args[3]);

    if (writeToFile) {
      fileShuffle(args[0], conf, parallel);
    } else {
      inMemoryShuffle(configuration, conf, parallel);
    }
  }

  private static void inMemoryShuffle(Configuration configuration, SparkConf conf, int parallel) {
    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaPairRDD<String, String> input = sc.newAPIHadoopRDD(configuration, StringInputFormat.class, String.class, String.class);
    JavaPairRDD<String, String> sorted = input.repartitionAndSortWithinPartitions(new HashPartitioner(parallel));
    sorted.saveAsHadoopFile("out", String.class, String.class, EmptyOutputFormat.class);
    sc.stop();
  }

  private static void fileShuffle(String file, SparkConf conf, int parallel) {
    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaRDD<String> input = sc.textFile(file + "/in");

    JavaPairRDD<String, String> source = input.mapToPair(new PairFunction<String, String, String>() {
      @Override
      public Tuple2<String, String> call(String s) throws Exception {
        String[] a = s.split(",");
        return new Tuple2<>(a[0], a[1]);
      }
    });

    source.repartitionAndSortWithinPartitions(new Partitioner() {
      @Override
      public int numPartitions() {
        return parallel;
      }

      @Override
      public int getPartition(Object key) {
        return (int) (Math.abs((long) key.toString().hashCode()) % parallel);
      }
    }).saveAsTextFile(file + "/sparkOut");
    sc.stop();
  }
}
