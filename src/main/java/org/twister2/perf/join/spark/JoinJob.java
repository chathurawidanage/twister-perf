package org.twister2.perf.join.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.logging.Logger;

public class JoinJob {

  private final static Logger LOG = Logger.getLogger(JoinJob.class.getName());

  public static void main(String[] args) {
    LOG.info("Starting join job....");
    SparkConf conf = new SparkConf().setAppName("join");
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    conf.registerKryoClasses(new Class[]{Integer.class, Long.class});

    Configuration configuration = new Configuration();

    JavaSparkContext sc = new JavaSparkContext(conf);

    JavaPairRDD<Integer, Long> input1 = sc.newAPIHadoopFile(args[0], KeyValueTextInputFormat.class, Text.class,
        Text.class, configuration).mapToPair((t) -> new Tuple2<>(Integer.valueOf(t._1.toString()), Long.valueOf(t._2.toString())));
    JavaPairRDD<Integer, Long> input2 = sc.newAPIHadoopFile(args[1], KeyValueTextInputFormat.class, Text.class,
        Text.class, configuration).mapToPair((t) -> new Tuple2<>(Integer.valueOf(t._1.toString()), Long.valueOf(t._2.toString())));

    JavaPairRDD<Integer, Tuple2<Long, Long>> joined = input1.join(input2);

    if (args.length > 2) {
      joined.saveAsTextFile(args[3]);
    }
    sc.stop();
    LOG.info("Stopping join job...");
  }
}
