package org.twister2.perf.join.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.twister2.perf.spark.ByteOutputFormat;
import org.twister2.perf.spark.tera.ByteInputFormat;
import org.twister2.perf.spark.tera.TeraSortPartitioner;
import org.twister2.perf.tws.Context;
import scala.Tuple2;

import java.math.BigInteger;

public class JoinJob {
  public static final String ARG_KEY_SIZE = "keySize";
  public static final String ARG_DATA_SIZE = "dataSize";

  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("join");
    Configuration configuration = new Configuration();

    double size = Double.parseDouble(args[0]);
    int keySize = Integer.parseInt(args[2]);
    int dataSize = Integer.parseInt(args[3]);
    int tuples = (int) (size * 1024 * 1024 * 1024 / (keySize + dataSize));
    boolean writeToFile = Boolean.parseBoolean(args[4]);

    configuration.set(Context.ARG_TUPLES, tuples + "");
    configuration.set(Context.ARG_PARALLEL, args[1]);
    configuration.set(ARG_KEY_SIZE, args[2]);
    configuration.set(ARG_DATA_SIZE, args[3]);


    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaPairRDD<byte[], byte[]> input1 = sc.newAPIHadoopRDD(configuration, ByteInputFormat.class, byte[].class, byte[].class);
    JavaPairRDD<byte[], byte[]> input2 = sc.newAPIHadoopRDD(configuration, ByteInputFormat.class, byte[].class, byte[].class);


    JavaPairRDD<byte[], Tuple2<Optional<byte[]>, byte[]>> joined = input1.rightOuterJoin(input2,
        new TeraSortPartitioner(
            input1.partitions().size()
        ));

    if (writeToFile) {
      joined.saveAsHadoopFile("out", byte[].class, byte[].class, ByteOutputFormat.class);
    } else {
      joined.map(t -> {
        return new BigInteger(t._1).toString() + " <" + new BigInteger(t._2._2) + "," + (t._2()._1.isPresent() ?
            new BigInteger(t._2()._1.get()).toString() : "NULL") + ">";
      }).saveAsTextFile(args[5]);
    }
    sc.stop();
  }
}
