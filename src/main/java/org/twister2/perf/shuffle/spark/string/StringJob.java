package org.twister2.perf.shuffle.spark.string;

import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class StringJob {
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("terasort");
    int parallel = Integer.parseInt(args[2]);
    fileShuffle(args[0], conf, parallel);
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
