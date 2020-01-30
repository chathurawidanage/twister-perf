package org.twister2.perf.join.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.twister2.perf.shuffle.Context;
import org.twister2.perf.shuffle.spark.tera.ByteInputFormat;

import java.util.logging.Logger;

public class JoinJobDataSetBinary {

  private final static Logger LOG = Logger.getLogger(JoinJobDataSetBinary.class.getName());

  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("join");
    Configuration configuration = new Configuration();

    double sizePerWorkerGB = Double.parseDouble(args[0]);
    int keySize = Integer.parseInt(args[2]);
    int dataSize = Integer.parseInt(args[3]);
    int tuples = (int) (sizePerWorkerGB * 1024 * 1024 * 1024 / (keySize + dataSize));
    System.out.println("Number of tuples to join: " + tuples);

    configuration.set(Context.ARG_TUPLES, tuples + "");
    configuration.set(Context.ARG_PARALLEL, args[1]);
    configuration.set(Context.ARG_KEY_SIZE, args[2]);
    configuration.set(Context.ARG_DATA_SIZE, args[3]);
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    conf.registerKryoClasses(new Class[]{byte[].class});

    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaPairRDD<byte[], byte[]> input1 = sc.newAPIHadoopRDD(configuration, ByteInputFormat.class, byte[].class, byte[].class);


    JavaPairRDD<byte[], byte[]> input2 = sc.newAPIHadoopRDD(configuration, ByteInputFormat.class, byte[].class, byte[].class);


    SQLContext sqlContext = new SQLContext(sc);
    Dataset<Row> ds1 = sqlContext.createDataset(JavaPairRDD.toRDD(input1),
        Encoders.tuple(Encoders.BINARY(), Encoders.BINARY())).toDF("key", "value");

    Dataset<Row> ds2 = sqlContext.createDataset(JavaPairRDD.toRDD(input2),
        Encoders.tuple(Encoders.BINARY(), Encoders.BINARY())).toDF("key", "value");

    Dataset<Row> join = ds1.alias("ds1").join(ds2.alias("ds2"), ds1.col("key")
        .equalTo(ds2.col("key")), "inner");


    join.foreach(r -> {

    });
    sc.stop();
    LOG.info("Stopping join job...");
  }
}
