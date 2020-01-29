package org.twister2.perf.shuffle.spark.tera;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.twister2.perf.shuffle.io.EmptyOutputFormat;
import org.twister2.perf.shuffle.Context;

public class TeraSortJob {

  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("terasort");
    Configuration configuration = new Configuration();

    int instances = conf.getInt("spark.executor.instances", 0);
    if (instances == 0) {
      System.out.println("Executor instances is zero. Exiting ....");
      return;
    } else {
      System.out.println("Executor instances: " + instances);
    }

    double sizePerWorkerGB = Double.parseDouble(args[0]);
    int keySize = Integer.parseInt(args[2]);
    int dataSize = Integer.parseInt(args[3]);
    int tuples = (int) (sizePerWorkerGB * 1024 * 1024 * 1024 / (keySize + dataSize));
    boolean writeToFile = Boolean.parseBoolean(args[4]);
    System.out.println("Number of tuples to sort: " + tuples);

    configuration.set(Context.ARG_TUPLES, tuples + "");
    configuration.set(Context.ARG_PARALLEL, args[1]);
    configuration.set(Context.ARG_KEY_SIZE, args[2]);
    configuration.set(Context.ARG_DATA_SIZE, args[3]);
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    conf.registerKryoClasses(new Class[]{byte[].class});

    JavaSparkContext sc = new JavaSparkContext(conf);
    long start = System.currentTimeMillis();
    JavaPairRDD<byte[], byte[]> input = sc.newAPIHadoopRDD(configuration, ByteInputFormat.class, byte[].class, byte[].class);
    JavaPairRDD<byte[], byte[]> sorted = input.repartitionAndSortWithinPartitions(new TeraSortPartitioner(input.partitions().size()), new ByteComparator());

    if (writeToFile) {
      sorted.saveAsTextFile(args[5]);
    } else {
      sorted.saveAsHadoopFile("out", byte[].class, byte[].class, EmptyOutputFormat.class);
    }
    long delay = System.currentTimeMillis() - start;
    System.out.println("SortingDelay ms " + delay);
    sc.stop();
  }

  public static boolean isSorted(JavaPairRDD<byte[], byte[]> sorted) {

    ByteComparator comparator = new ByteComparator();

    int counter = 0;
    scala.Tuple2 pt = sorted.first();
    for (scala.Tuple2 t : sorted.collect()) {
      if(comparator.compare( (byte[])pt._1, (byte[]) t._1) > 0) {
        System.out.println("first is larger than the second");
        counter++;
      }

      pt = t;
    }

    return counter == 0;
  }
}
