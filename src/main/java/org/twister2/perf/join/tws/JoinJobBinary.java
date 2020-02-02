package org.twister2.perf.join.tws;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Job;
import edu.iu.dsc.tws.api.comms.CommunicationContext;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;
import edu.iu.dsc.tws.api.comms.structs.JoinedTuple;
import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.tset.TSetContext;
import edu.iu.dsc.tws.api.tset.fn.BaseSinkFunc;
import edu.iu.dsc.tws.api.tset.fn.MapFunc;
import edu.iu.dsc.tws.api.tset.fn.PartitionFunc;
import edu.iu.dsc.tws.api.tset.fn.SourceFunc;
import edu.iu.dsc.tws.api.tset.schema.KeyedSchema;
import edu.iu.dsc.tws.api.tset.schema.TupleSchema;
import edu.iu.dsc.tws.data.utils.HdfsDataContext;
import edu.iu.dsc.tws.rsched.job.Twister2Submitter;
import edu.iu.dsc.tws.tset.env.BatchTSetEnvironment;
import edu.iu.dsc.tws.tset.links.batch.JoinTLink;
import edu.iu.dsc.tws.tset.sets.batch.KeyedSourceTSet;
import edu.iu.dsc.tws.tset.sets.batch.SinkTSet;
import edu.iu.dsc.tws.tset.worker.BatchTSetIWorker;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.twister2.perf.shuffle.spark.tera.ByteComparator;
import org.twister2.perf.shuffle.spark.tera.TeraSortPartitioner;

import java.io.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class JoinJobBinary implements BatchTSetIWorker, Serializable {

  private static final Logger LOG = Logger.getLogger(JoinJobBinary.class.getName());

  private static final String CONFIG_PARALLELISM = "CONFIG_PARALLELISM";
  private static final String CONFIG_MEMORY = "CONFIG_MEMORY";

  private static final String SIZE_PER_WORKER = "SIZE_PER_WORKER";
  private static final String KEY_SIZE = "KEY_SIZE";

  private static final String CONFIG_ALGO = "CONFIG_ALGO";
  private static final String CONFIG_DISK = "CONFIG_DISK";
  private static final String VALUE_SIZE = "VALUE_SIZE";

  @Override
  public void execute(BatchTSetEnvironment env) {
    LOG.info("Starting execution....");
    double sizePerWorkerGB = Double.parseDouble(env.getConfig().getStringValue(SIZE_PER_WORKER));
    int keySize = Integer.parseInt(env.getConfig().getStringValue(KEY_SIZE));
    int dataSize = Integer.parseInt(env.getConfig().getStringValue(VALUE_SIZE));
    int tuples = (int) (sizePerWorkerGB * 1024 * 1024 * 1024 / (keySize + dataSize));

    int parallelism = env.getConfig().getIntegerValue(CONFIG_PARALLELISM);

    LOG.info("Creating sources...");
    TupleSchema schema = new KeyedSchema(MessageTypes.BYTE_ARRAY, MessageTypes.BYTE_ARRAY);


    class SourceFuncClass implements SourceFunc<Tuple<byte[], byte[]>> {

      private Random random;
      private long generatedTuples = 0;
      private byte[] value;

      SourceFuncClass() {
        this.random = new Random(System.nanoTime());
        this.value = new byte[dataSize];
        this.random.nextBytes(value);
      }

      @Override
      public boolean hasNext() {
        return this.generatedTuples < tuples;
      }

      @Override
      public Tuple<byte[], byte[]> next() {
        this.generatedTuples++;
        byte[] key = new byte[keySize];
        this.random.nextBytes(key);
        return Tuple.of(key, value);
      }
    }

    KeyedSourceTSet<byte[], byte[]> source1 = env.createKeyedSource(new SourceFuncClass(), parallelism)
        .withSchema(schema);

    KeyedSourceTSet<byte[], byte[]> source2 = env.createKeyedSource(new SourceFuncClass(), parallelism)
        .withSchema(schema);


    LOG.info("Joining...");
    JoinTLink<byte[], byte[], byte[]> joined = source1.join(source2,
        CommunicationContext.JoinType.INNER, new ByteComparator(), new TaskPartitionerForRandom());

    if (env.getConfig().getBooleanValue(CONFIG_DISK)) {
      joined.useDisk();
    }

    if (env.getConfig().getStringValue(CONFIG_ALGO).equals("hash")) {
      joined.useHashAlgorithm();
    }

    SinkTSet<Iterator<JoinedTuple<byte[], byte[], byte[]>>> sink = joined.sink(new BaseSinkFunc<Iterator<JoinedTuple<byte[], byte[], byte[]>>>() {


      @Override
      public void prepare(TSetContext ctx) {
        super.prepare(ctx);
      }

      @Override
      public boolean add(Iterator<JoinedTuple<byte[], byte[], byte[]>> values) {

        long count = 0;
        long t1 = System.currentTimeMillis();
        while (values.hasNext()) {
          JoinedTuple<byte[], byte[], byte[]> next = values.next();
          count++;
        }
        LOG.info("Join performed and produced " + count + " iteration took : "
            + (System.currentTimeMillis() - t1));

        return true;
      }
    });
    env.run(sink);
  }

  public static void main(String[] args) {
    JobConfig jobConfig = new JobConfig();

    jobConfig.put(CONFIG_PARALLELISM, Integer.parseInt(args[0]));
    //1 memory
    jobConfig.put(SIZE_PER_WORKER, args[2]);
    jobConfig.put(KEY_SIZE, args[3]);
    jobConfig.put(VALUE_SIZE, args[4]);

    jobConfig.put(CONFIG_ALGO, args[5]);
    jobConfig.put(CONFIG_DISK, Boolean.parseBoolean(args[6]));

    Twister2Job job = Twister2Job.newBuilder().setJobName("join-job")
        .setWorkerClass(JoinJobBinary.class)
        .addComputeResource(1, Integer.parseInt(args[1]), Integer.parseInt(args[0]))
        .setConfig(jobConfig)
        .build();

    Twister2Submitter.submitJob(job);
  }
}
