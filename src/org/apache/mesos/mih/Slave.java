package org.apache.mesos.mih;

import java.io.*;
import java.net.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class Slave {
  private JobConf conf;
  private Process proc;

  public Slave(JobConf conf) {
    this.conf = conf;
  }

  public void run() throws IOException {
    // Read the master mapper's address from the HDFS file it creates
    FileSystem fs = FileSystem.get(conf);
    Path path = MIH.getMasterFile(conf);
    String ip = null;
    int port = 0;
    while (true) {
      try {
        Thread.sleep((int) (500 + Math.random() * 1000));
      } catch (InterruptedException e) {}
      try {
        if (fs.getFileStatus(path).getLen() > 0) {
          FSDataInputStream in = fs.open(path);
          BufferedReader br = new BufferedReader(new InputStreamReader(in));
          String address = br.readLine();
          String[] pieces = address.split(":");
          ip = pieces[0];
          port = Integer.parseInt(pieces[1]);
          break;
        }
      } catch (IOException e) {}
    }

    // Connect to the master and tell it our address
    Socket sock = new Socket(ip, port);
    DataOutputStream out = new DataOutputStream(sock.getOutputStream());
    DataInputStream in = new DataInputStream(sock.getInputStream());
    String myAddress = InetAddress.getLocalHost().getHostAddress();
    out.writeUTF(myAddress);
    out.flush();

    // Wait for commands from it
    while (true) {
      String command = in.readUTF();
      if (command.equals("run-slave")) {
        String mesosUrl = in.readUTF();
        int numSlots = in.readInt();
        System.out.println("Launching slave with URL " + mesosUrl + " and " + numSlots + " cores");
        launchMesosSlave(mesosUrl, numSlots);
      }
    }
  }
  
  // Start mesos-slave with a given master URL and number of Hadoop slots
  void launchMesosSlave(String mesosUrl, int numSlots) throws IOException {
    String mesosHome = MIH.getMesosHome(conf);
    int memPerSlot = MIH.getMemPerSlot(conf);
    String[] command = new String[] {
      mesosHome + "/bin/mesos-slave",
      "--url=" + mesosUrl,
      "--resources=cpus:" + numSlots + ";mem:" + (memPerSlot * numSlots)
    };
    proc = Runtime.getRuntime().exec(command);

    // Start a thread to redirect the process's stdout to ours
    new Thread("stdout redirector for mesos-slave") {
      public void run() {
        try {
          BufferedReader in = new BufferedReader(new InputStreamReader(proc.getInputStream()));
          String line = null;
          while ((line = in.readLine()) != null) {
            System.out.println(line);
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }.start();

    // Start a thread to redirect the process's stderr to ours
    new Thread("stderr redirector for mesos-slave") {
      public void run() {
        try {
          BufferedReader in = new BufferedReader(new InputStreamReader(proc.getErrorStream()));
          String line = null;
          while ((line = in.readLine()) != null) {
            System.err.println(line);
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }.start();
  }

  public void stop() {
    if (proc != null) {
      System.out.println("Stopping mesos-slave");
      proc.destroy();
    }
  }
}
