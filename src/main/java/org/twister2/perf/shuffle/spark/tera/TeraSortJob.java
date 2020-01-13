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

    double size = Double.parseDouble(args[0]);
    int keySize = Integer.parseInt(args[2]);
    int dataSize = Integer.parseInt(args[3]);
    int tuples = (int) (size * 1024 * 1024 * 1024 / (keySize + dataSize));
    boolean writeToFile = Boolean.parseBoolean(args[4]);

    configuration.set(Context.ARG_TUPLES, tuples + "");
    configuration.set(Context.ARG_PARALLEL, args[1]);
    configuration.set(Context.ARG_KEY_SIZE, args[2]);
    configuration.set(Context.ARG_DATA_SIZE, args[3]);
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    conf.registerKryoClasses(new Class[]{byte[].class});

    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaPairRDD<byte[], byte[]> input = sc.newAPIHadoopRDD(configuration, ByteInputFormat.class, byte[].class, byte[].class);
    JavaPairRDD<byte[], byte[]> sorted = input.repartitionAndSortWithinPartitions(new TeraSortPartitioner(input.partitions().size()), new ByteComparator());

    if (writeToFile) {
      sorted.saveAsHadoopFile("out", byte[].class, byte[].class, EmptyOutputFormat.class);
    } else {
      sorted.saveAsTextFile(args[5]);
    }
    sc.stop();
  }
}
