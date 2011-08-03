package org.apache.mesos.mih;

import java.io.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.NullOutputFormat;

public class MIH {
  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      System.err.println("Usage: mih <numSlots>");
      System.exit(1);
    }
    int numTasks = Integer.parseInt(args[0]);

    String mesosHome = System.getenv("MESOS_HOME");
    if (mesosHome == null) {
      System.err.println("MESOS_HOME environment variable is not set");
      // TODO: Use /usr/local/mesos by default?
    }

    // Submit the job
    JobConf conf = new JobConf(MIH.class);
    conf.setJobName("MesosInHadoop");
    conf.setMapperClass(Map.class);
    conf.setInputFormat(DummyInputFormat.class);
    conf.setOutputFormat(NullOutputFormat.class);
    conf.setMapOutputKeyClass(NullWritable.class);
    conf.setMapOutputValueClass(NullWritable.class);
    conf.setNumMapTasks(numTasks);
    conf.setNumReduceTasks(0);
    conf.setSpeculativeExecution(false);
    conf.set("mesos.home", mesosHome);
    JobClient client = new JobClient(conf);
    RunningJob job = client.submitJob(conf);
    System.out.println("Submitted job to Hadoop; waiting for it to start");

    // Wait for the Mesos URL file to be created to tell it to the user
    conf.set("mapred.job.id", job.getID().toString());
    FileSystem fs = FileSystem.get(conf);
    Path path = MIH.getMesosUrlFile(conf);
    while (true) {
      try {
        Thread.sleep((int) (500 + Math.random() * 1000));
      } catch (InterruptedException e) {}
      try {
        if (fs.getFileStatus(path).getLen() > 0) {
          FSDataInputStream in = fs.open(path);
          BufferedReader br = new BufferedReader(new InputStreamReader(in));
          String url = br.readLine();
          String spaces = "        ";
          System.out.println("Mesos started at:\n" + spaces + url);
          System.out.println("To stop the cluster, use:\n" + spaces + "hadoop job -kill " + job.getID());
          System.exit(0);
          break;
        }
      } catch (IOException e) {}
    }
  }

  public static class Map extends MapReduceBase
      implements Mapper<IntWritable, IntWritable, NullWritable, NullWritable> {
    private JobConf conf;
    private Master master = null;
    private Slave slave = null;

    @Override
    public void configure(JobConf conf) {
      this.conf = conf;
    }

    @Override
    public void map(IntWritable key, IntWritable value, 
        OutputCollector<NullWritable, NullWritable> out, Reporter reporter)
        throws IOException {
      // Check our task ID. If we are task 0, we're the leader and we get to
      // launch the Mesos master. In addition, every mapper also runs a slave.
      int taskId = conf.getInt("mapred.task.partition", -1);
      System.out.println("In map(), task ID = " + taskId);
      if (taskId == 0) {
        new Thread("master runner") {
          public void run() {
            try {
              master = new Master(conf);
              master.run();
            } catch (IOException e) {
              System.err.println("Error in master runner thread:");
              e.printStackTrace();
            }
          }
        }.start();
      }
      // Start the slave
      new Thread("slave runner") {
        public void run() {
          try {
            slave = new Slave(conf);
            slave.run();
          } catch (IOException e) {
            System.err.println("Error in slave runner thread:");
            e.printStackTrace();
          }
        }
      }.start();
      // Report progress to prevent our task from being timed out
      while (true) {
        try {
          Thread.sleep(10000);
        } catch (InterruptedException e) {}
        reporter.progress();
      }
    }

    @Override
    public void close() {
      if (slave != null)
        slave.stop();
      if (master != null)
        master.stop();
    }
  }

  // A dummy InputFormat that has one key/value record in each split.
  public static class DummyInputFormat extends Configured
      implements InputFormat<IntWritable,IntWritable> {
    public InputSplit[] getSplits(JobConf conf, int numSplits) {
      InputSplit[] ret = new InputSplit[numSplits];
      for (int i = 0; i < numSplits; i++) {
        ret[i] = new DummySplit();
      }
      return ret;
    }

    @Override
    public RecordReader<IntWritable,IntWritable> getRecordReader(
        InputSplit ignored, JobConf conf, Reporter reporter)
      throws IOException {
      return new RecordReader<IntWritable,IntWritable>() {
        boolean readSomething = false;
        public boolean next(IntWritable key, IntWritable value)
          throws IOException {
          if (readSomething) {
            return false;
          } else {
            key.set(1);
            value.set(1);
            readSomething = true;
            return true;
          }
        }
        public IntWritable createKey() { return new IntWritable(); }
        public IntWritable createValue() { return new IntWritable(); }
        public long getPos() throws IOException { return 0; }
        public void close() throws IOException { }
        public float getProgress() throws IOException {
          return 1.0f;
        }
      };
    }
  }

  public static class DummySplit implements InputSplit {
    public void write(DataOutput out) throws IOException { }
    public void readFields(DataInput in) throws IOException { }
    public long getLength() { return 0L; }
    public String[] getLocations() { return new String[0]; }
  }

  public static Path getMasterFile(JobConf conf) {
    String jobId = conf.get("mapred.job.id");
    String tmpDir = conf.get("hadoop.tmp.dir");
    return new Path(tmpDir, jobId + "_mih_coordinator");
  }

  public static Path getMesosUrlFile(JobConf conf) {
    String jobId = conf.get("mapred.job.id");
    String tmpDir = conf.get("hadoop.tmp.dir");
    return new Path(tmpDir, jobId + "_mih_url");
  }

  public static String getMesosHome(JobConf conf) {
    return conf.get("mesos.home");
  }
}
