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
import edu.iu.dsc.tws.data.utils.HdfsDataContext;
import edu.iu.dsc.tws.rsched.job.Twister2Submitter;
import edu.iu.dsc.tws.tset.env.BatchTSetEnvironment;
import edu.iu.dsc.tws.tset.fn.HashingPartitioner;
import edu.iu.dsc.tws.tset.links.batch.JoinTLink;
import edu.iu.dsc.tws.tset.sets.batch.KeyedSourceTSet;
import edu.iu.dsc.tws.tset.sets.batch.KeyedTSet;
import edu.iu.dsc.tws.tset.sets.batch.SinkTSet;
import edu.iu.dsc.tws.tset.sets.batch.SourceTSet;
import edu.iu.dsc.tws.tset.worker.BatchTSetIWorker;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

import java.io.*;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

public class JoinJob implements BatchTSetIWorker, Serializable {

  private static final Logger LOG = Logger.getLogger(JoinJob.class.getName());

  private static final String CONFIG_PARALLELISM = "CONFIG_PARALLELISM";
  private static final String CONFIG_MEMORY = "CONFIG_MEMORY";

  private static final String CONFIG_OUT_PATH = "CONFIG_OUT_PATH";
  private static final String CONFIG_WRITE_TO_FILE = "CONFIG_WRITE_TO_FILE";

  private static final String CONFIG_INPUT1_PATH = "CONFIG_INPUT1_PATH";
  private static final String CONFIG_INPUT2_PATH = "CONFIG_INPUT2_PATH";

  @Override
  public void execute(BatchTSetEnvironment env) {
    LOG.info("Starting execution....");
    Configuration configuration1 = new Configuration();
    configuration1.addResource(
        new Path(HdfsDataContext.getHdfsConfigDirectory(env.getConfig())));
    configuration1.set(KeyValueTextInputFormat.INPUT_DIR,
        env.getConfig().getStringValue(CONFIG_INPUT1_PATH));

    Configuration configuration2 = new Configuration();
    configuration2.addResource(
        new Path(HdfsDataContext.getHdfsConfigDirectory(env.getConfig())));
    configuration2.set(KeyValueTextInputFormat.INPUT_DIR,
        env.getConfig().getStringValue(CONFIG_INPUT2_PATH));

    int parallelism = env.getConfig().getIntegerValue(CONFIG_PARALLELISM);

    LOG.info("Creating sources...");
    SourceTSet<Tuple<Integer, Long>> source1 = env.createHadoopSource(configuration1,
        KeyValueTextInputFormat.class, parallelism,
        (MapFunc<Tuple<Integer, Long>, Tuple<Text, Text>>) input ->
            Tuple.of(Integer.parseInt(input.getKey().toString()), Long.parseLong(input.getValue().toString())));

    SourceTSet<Tuple<Integer, Long>> source2 = env.createHadoopSource(configuration2,
        KeyValueTextInputFormat.class, parallelism, (MapFunc<Tuple<Integer, Long>, Tuple<Text, Text>>) input ->
            Tuple.of(Integer.parseInt(input.getKey().toString()), Long.parseLong(input.getValue().toString())));

    KeyedTSet<Integer, Long> keyedTSet1 = source1.mapToTuple(input -> input);
    KeyedTSet<Integer, Long> keyedTSet2 = source2.mapToTuple(input -> input);

    LOG.info("Joining...");
    JoinTLink<Integer, Long, Long> joined = keyedTSet1.join(keyedTSet2,
        CommunicationContext.JoinType.INNER, null, new HashingPartitioner<>())
        .useDisk().useHashAlgorithm(MessageTypes.INTEGER);

    SinkTSet<Iterator<JoinedTuple<Integer, Long, Long>>> sink = joined.sink(new BaseSinkFunc<Iterator<JoinedTuple<Integer, Long, Long>>>() {

      private String fileName;
      private boolean writeToFile = false;

      @Override
      public void prepare(TSetContext ctx) {
        super.prepare(ctx);
        this.writeToFile = ctx.getConfig().getBooleanValue(CONFIG_WRITE_TO_FILE);
        String outPath = ctx.getConfig().getStringValue(CONFIG_OUT_PATH);
        this.fileName = outPath + "/out-" + ctx.getIndex();
      }

      @Override
      public boolean add(Iterator<JoinedTuple<Integer, Long, Long>> values) {
        if (this.writeToFile) {
          try (BufferedWriter bufferedWriter = new BufferedWriter(
              new FileWriter(new File(this.fileName)))) {
            while (values.hasNext()) {
              JoinedTuple<Integer, Long, Long> next = values.next();
              bufferedWriter.write(String.format("(%d)(%d,%d)", next.getKey(),
                  next.getLeftValue(), next.getRightValue()));
              bufferedWriter.newLine();
            }
          } catch (IOException iex) {
            LOG.log(Level.SEVERE, "Failed to write to file", iex);
          }
        }
        return true;
      }
    });
    env.run(sink);
  }

  public static void main(String[] args) {
    JobConfig jobConfig = new JobConfig();

    jobConfig.put(CONFIG_PARALLELISM, Integer.parseInt(args[0]));
    //1 memory
    jobConfig.put(CONFIG_INPUT1_PATH, args[2]);
    jobConfig.put(CONFIG_INPUT2_PATH, args[3]);
    jobConfig.put(CONFIG_WRITE_TO_FILE, Boolean.parseBoolean(args[4]));
    jobConfig.put(CONFIG_OUT_PATH, args[5]);

    Twister2Job job = Twister2Job.newBuilder().setJobName("join-job")
        .setWorkerClass(JoinJob.class)
        .addComputeResource(1, Integer.parseInt(args[1]), Integer.parseInt(args[0]))
        .setConfig(jobConfig)
        .build();

    Twister2Submitter.submitJob(job);
  }
}
