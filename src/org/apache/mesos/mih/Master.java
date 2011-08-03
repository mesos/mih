package org.apache.mesos.mih;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class Master {
  private JobConf conf;
  private Process proc;

  public Master(JobConf conf) {
    this.conf = conf;
  }

  public void run() throws IOException {
    // Listen on a local port for connecting slaves
    ServerSocket serverSock = new ServerSocket(0, 500);
    String ip = InetAddress.getLocalHost().getHostAddress();
    int port = serverSock.getLocalPort();
    
    // Write our address in HDFS for the other mappers to find it
    FileSystem fs = FileSystem.get(conf);
    Path path = MIH.getMasterFile(conf);
    FSDataOutputStream fileOut = fs.create(path, false);
    fileOut.writeBytes(ip + ":" + port + "\n");
    fileOut.close();

    // Accept connections until we have as many as the number of map tasks
    HashMap<String, ArrayList<SlaveConnection>> connsByHost =
      new HashMap<String, ArrayList<SlaveConnection>>();
    int numConnected = 0;
    int numMaps = conf.getInt("mapred.map.tasks", -1);
    while (numConnected < numMaps) {
      Socket sock = serverSock.accept();
      try {
        DataInputStream in = new DataInputStream(sock.getInputStream());
        String host = in.readUTF();
        ArrayList<SlaveConnection> conns = connsByHost.get(host);
        if (conns == null) {
          conns = new ArrayList<SlaveConnection>();
          connsByHost.put(host, conns);
        }
        conns.add(new SlaveConnection(sock, host));
        numConnected++;
      } catch (IOException e) {
        // For now we assume that this means a map task was killed, and we wait for
        // it to be relaunched. Might be nice to do something more robust later.
        System.err.println("Exception when trying to read a slave connection:");
        e.printStackTrace();
      }
    }

    // Launch the Mesos master
    String mesosUrl = launchMesosMaster();

    // Tell the first mapper on each host to run mesos-slave with the right # of cores;
    // the rest of the mappers will just wait
    for (Map.Entry<String, ArrayList<SlaveConnection>> e: connsByHost.entrySet()) {
      int numSlots = e.getValue().size();
      Socket sock = e.getValue().get(0).socket;
      DataOutputStream out = new DataOutputStream(sock.getOutputStream());
      out.writeUTF("run-slave");
      out.writeUTF(mesosUrl);
      out.writeInt(numSlots);
      out.flush();
    }

    // Write the Mesos URL in HDFS so the job client can find it
    Path urlPath = MIH.getMesosUrlFile(conf);
    FSDataOutputStream urlOut = fs.create(urlPath, false);
    urlOut.writeBytes(mesosUrl + "\n");
    urlOut.close();

    // Loop so that the map task doesn't finish (TODO: need a way to shut down cluster)
    while (true) {
      try { Thread.sleep(2000); } catch (InterruptedException e) {}
    }
  }

  // Start mesos-master and returns its URL
  String launchMesosMaster() throws IOException {
    String mesosHome = MIH.getMesosHome(conf);
    String[] command = new String[] {mesosHome + "/bin/mesos-master", "--port=0"};
    proc = Runtime.getRuntime().exec(command);
    final SynchronousQueue<String> urlQueue = new SynchronousQueue<String>();

    // Start a thread to redirect the process's stdout to ours
    new Thread("stdout redirector for mesos-master") {
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

    // Start a thread to redirect the process's stderr to ours and also read the URL
    new Thread("stderr redirector for mesos-master") {
      public void run() {
        try {
          BufferedReader in = new BufferedReader(new InputStreamReader(proc.getErrorStream()));
          String line = null;
          boolean foundUrl = false;
          Pattern pattern = Pattern.compile(".*Master started at (.*)");
          while ((line = in.readLine()) != null) {
            System.err.println(line);
            if (!foundUrl) {
              Matcher m = pattern.matcher(line);
              if (m.matches()) {
                String url = m.group(1);
                urlQueue.put(url);
                foundUrl = true;
              }
            }
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }.start();
    
    // Wait for the URL to be read
    String url = null;
    while (url == null) {
      try {
        url = urlQueue.take();
      } catch (InterruptedException e) {}
    }
    System.out.println("Mesos master started with URL " + url);
    try {
      Thread.sleep(500); // Give mesos-master a bit more time to start up
    } catch (InterruptedException e) {}
    return url;
  }

  public void stop() {
    if (proc != null) {
      System.out.println("Stopping mesos-slave");
      proc.destroy();
    }
  }

  static class SlaveConnection {
    Socket socket;
    String host;

    SlaveConnection(Socket socket, String host) {
      this.socket = socket;
      this.host = host;
    }
  }
}
