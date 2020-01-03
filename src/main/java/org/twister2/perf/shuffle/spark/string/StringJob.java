package org.twister2.perf.shuffle.spark.string;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.twister2.perf.shuffle.Context;
import scala.Tuple2;

public class StringJob {
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("terasort");
    Configuration configuration = new Configuration();
    configuration.set(Context.ARG_PARALLEL, args[1]);
    int parallel = Integer.parseInt(args[1]);

    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaRDD<String> input = sc.textFile(args[0] + "/in");

    JavaPairRDD<String, String>  source = input.mapToPair(new PairFunction<String, String, String>() {
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
    }).saveAsTextFile(args[0] + "/sparkOut");
    sc.stop();
  }
}
