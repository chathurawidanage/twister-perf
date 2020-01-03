package org.twister2.perf.shuffle.tws;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Job;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.resource.IWorker;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.job.Twister2Submitter;
import org.twister2.perf.shuffle.tws.string.StringWriter;
import org.twister2.perf.shuffle.Context;
import org.twister2.perf.shuffle.tws.bigint.WritingJob;

import java.util.HashMap;

public class Writer {
  public static void main(String[] args) {
    Config config = ResourceAllocator.loadConfig(new HashMap<>());
    String filePrefix = args[0];
    int parallel = Integer.parseInt(args[1]);
    int memory = Integer.parseInt(args[2]);
    boolean csv = Boolean.parseBoolean(args[3]);
    int tuples = Integer.parseInt(args[4]);
    int keySize = Integer.parseInt(args[5]);
    int dataSize = Integer.parseInt(args[6]);
    String writer = args[7];

    JobConfig jobConfig = new JobConfig();

    Class<? extends IWorker> c;
    if (writer.equals("string")) {
      c = StringWriter.class;
    } else if (writer.equals("bigint")) {
      c = WritingJob.class;
    } else {
      throw new RuntimeException("Failed to recognize option: " + writer);
    }

    jobConfig.put(Context.ARG_FILE_PREFIX, filePrefix);
    jobConfig.put(Context.ARG_CSV, csv);
    jobConfig.put(Context.ARG_TUPLES, tuples);
    jobConfig.put(Context.ARG_KEY_SIZE, keySize);
    jobConfig.put(Context.ARG_DATA_SIZE, dataSize);

    Twister2Job twister2Job;
    twister2Job = Twister2Job.newBuilder()
        .setJobName(WritingJob.class.getName())
        .setWorkerClass(c)
        .addComputeResource(1, memory, parallel)
        .setConfig(jobConfig)
        .build();
    // now submit the job
    Twister2Submitter.submitJob(twister2Job, config);
  }
}
