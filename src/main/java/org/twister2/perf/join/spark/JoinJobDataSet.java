package org.twister2.perf.join.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;

import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

public class JoinJobDataSet {

  private final static Logger LOG = Logger.getLogger(JoinJobDataSet.class.getName());

  public static void main(String[] args) {
    int partitions = Integer.parseInt(args[0]);

    LOG.info("Starting join job....");
    SparkConf conf = new SparkConf().setAppName("join");
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    conf.registerKryoClasses(new Class[]{Integer.class, Long.class});

    Configuration configuration = new Configuration();

    JavaSparkContext sc = new JavaSparkContext(conf);

    JavaPairRDD<Integer, Long> input1 = sc.newAPIHadoopFile(args[1], KeyValueTextInputFormat.class, Text.class,
        Text.class, configuration).mapToPair((t) -> new Tuple2<>(Integer.valueOf(t._1.toString()), Long.valueOf(t._2.toString())));

    LOG.info("No of Partitions of input 1 : " + input1.getNumPartitions());

    JavaPairRDD<Integer, Long> input2 = sc.newAPIHadoopFile(args[2], KeyValueTextInputFormat.class, Text.class,
        Text.class, configuration).mapToPair((t) -> new Tuple2<>(Integer.valueOf(t._1.toString()), Long.valueOf(t._2.toString())));


    SQLContext sqlContext = new SQLContext(sc);
    Dataset<Row> ds1 = sqlContext.createDataset(JavaPairRDD.toRDD(input1),
        Encoders.tuple(Encoders.INT(), Encoders.LONG())).toDF("key", "value");

    Dataset<Row> ds2 = sqlContext.createDataset(JavaPairRDD.toRDD(input2),
        Encoders.tuple(Encoders.INT(), Encoders.LONG())).toDF("key", "value");

    Dataset<Row> join = ds1.alias("ds1").join(ds2.alias("ds2"), ds1.col("key")
        .equalTo(ds2.col("key")), "inner");

    if (args.length > 3) {
      join.write().text(args[3]);
    } else {
      join.foreach(r -> {
        Integer key = r.getInt(0);
        Long v1 = r.getLong(1);
        Long v2 = r.getLong(3);
      });
    }
    sc.stop();
    LOG.info("Stopping join job...");
  }
}
