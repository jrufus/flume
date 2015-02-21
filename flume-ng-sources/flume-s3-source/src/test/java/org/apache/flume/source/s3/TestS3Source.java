/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.flume.source.s3;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;

import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.collect.Lists;
import org.apache.flume.Channel;
import org.apache.flume.ChannelException;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.lifecycle.LifecycleController;
import org.apache.flume.lifecycle.LifecycleState;
import org.apache.flume.source.s3.S3SourceConfigurationConstants;
import org.apache.http.client.methods.HttpRequestBase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

public class TestS3Source {
  static S3Source source;
  static MemoryChannel channel;
  private File tmpDir;

  @Before
  public void setUp() {
    source = new S3Source();
    channel = new MemoryChannel();

    Configurables.configure(channel, new Context());

    List<Channel> channels = new ArrayList<Channel>();
    channels.add(channel);

    ChannelSelector rcs = new ReplicatingChannelSelector();
    rcs.setChannels(channels);

    source.setChannelProcessor(new ChannelProcessor(rcs));
    tmpDir = Files.createTempDir();
  }

  @After
  public void tearDown() {
    for (File f : tmpDir.listFiles()) {
      f.delete();
    }
    tmpDir.delete();
  }

  @Test(timeout=30000)
  public void testS3Source() throws Exception {
    testS3Source(false);
  }

  @Test(timeout=30000)
  public void testS3SourceRestart() throws Exception {
    testS3Source(true);
  }

  private void testS3Source(boolean restart) throws Exception {
    Context chContext = new Context();
    chContext.put("capacity", "500");
    chContext.put("transactionCapacity", "500");
    chContext.put("keep-alive", "0");
    Configurables.configure(channel, chContext);

    channel.start();
    Context context = new Context();
    String bucketName = "filesbucket";
    File bucketDir = new File(tmpDir, bucketName);
    if(bucketDir.exists()) {
      bucketDir.delete();
    }
    bucketDir.mkdir();

    int numFiles = 50;
    List<File> files = new ArrayList<File>();
    for(int i = 0; i < numFiles; i++) {
      File f1 = new File(bucketDir.getAbsolutePath() + "/file"+i);
      StringBuilder strb = new StringBuilder();
      for(int j = 0; j < i; j++) {
        strb.append("file"+i+"line"+j+"\n");
      }

      Files.write("file1line1\nfile1line2\nfile1line3\nfile1line4\n" +
                      "file1line5\nfile1line6\nfile1line7\nfile1line8\n",
              f1, Charsets.UTF_8);

      files.add(f1);
    }

    context.put(S3SourceConfigurationConstants.BACKING_DIR, tmpDir.getAbsolutePath());
    context.put(S3SourceConfigurationConstants.ACCESS_KEY, "");
    context.put(S3SourceConfigurationConstants.SECRET_KEY, "");
    context.put(S3SourceConfigurationConstants.BUCKET_NAME, bucketName);
    context.put(S3SourceConfigurationConstants.BATCH_SIZE, "5");

    //context.put(S3SourceConfigurationConstants.ACCESS_KEY, "AKIAIACMXFTEW2G2JLMQ");
    //context.put(S3SourceConfigurationConstants.SECRET_KEY, "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");
    //context.put(S3SourceConfigurationConstants.BUCKET_NAME, "jrufusbucket1");

    Configurables.configure(source, context);
    source.setBackOff(false);
    source.setS3Client(new TestAmazonS3Client(tmpDir.getAbsolutePath()));
    source.start();

    List<String> dataOut = Lists.newArrayList();
    int i = 0;
    Transaction tx = channel.getTransaction();
    tx.begin();
    while(i < 50) {
      Event e = channel.take();
      if (e != null) {
        dataOut.add(new String(e.getBody(), "UTF-8"));
        i++;
        //System.out.println(new String(e.getBody(), "UTF-8"));
      }
    }
    if(restart) {
      tx.commit();
      tx.close();
      source.stop();
      channel.stop();
      channel.start();
      source.start();
      tx = channel.getTransaction();
      tx.begin();
    }
    while(i < 400) {
      Event e = channel.take();
      if (e != null) {
        dataOut.add(new String(e.getBody(), "UTF-8"));
        i++;
        System.out.println(new String(e.getBody(), "UTF-8"));
      }
    }
    tx.commit();
    tx.close();

    // Successfully  reaching here w/o causing a test timeout
    // ensures we have all the 50 files processed and received the 400 events
  }



  @Test
  public void testSourceDoesNotDieOnFullChannel() throws Exception {

    Context chContext = new Context();
    chContext.put("capacity", "2");
    chContext.put("transactionCapacity", "2");
    chContext.put("keep-alive", "0");
    channel.stop();
    Configurables.configure(channel, chContext);

    channel.start();
    Context context = new Context();

    String bucketName = "filesbucket";
    File bucketDir = new File(tmpDir, bucketName);
    if(bucketDir.exists()) {
      bucketDir.delete();
    }
    bucketDir.mkdir();

    File f1 = new File(bucketDir.getAbsolutePath() + "/file1");
    Files.write("file1line1\nfile1line2\nfile1line3\nfile1line4\n" +
                    "file1line5\nfile1line6\nfile1line7\nfile1line8\n",
            f1, Charsets.UTF_8);

    context.put(S3SourceConfigurationConstants.BACKING_DIR, tmpDir.getAbsolutePath());
    context.put(S3SourceConfigurationConstants.ACCESS_KEY, "");
    context.put(S3SourceConfigurationConstants.SECRET_KEY, "");
    context.put(S3SourceConfigurationConstants.BUCKET_NAME, bucketName);
    context.put(S3SourceConfigurationConstants.BATCH_SIZE, "2");

    Configurables.configure(source, context);
    source.setBackOff(false);
    source.setS3Client(new TestAmazonS3Client(tmpDir.getAbsolutePath()));
    source.start();

    // Wait for the source to read enough events to fill up the channel.
    while(!source.hitChannelException()) {
      Thread.sleep(50);
    }

    List<String> dataOut = Lists.newArrayList();

    for (int i = 0; i < 8; ) {
      Transaction tx = channel.getTransaction();
      tx.begin();
      Event e = channel.take();
      if (e != null) {
        System.out.println("Event taken is **************** "+e.getBody());
        dataOut.add(new String(e.getBody(), "UTF-8"));
        i++;
      }
      e = channel.take();
      if (e != null) {
        System.out.println("Event taken is **************** "+e.getBody());
        dataOut.add(new String(e.getBody(), "UTF-8"));
        i++;
      }
      tx.commit();
      tx.close();
    }
    Assert.assertTrue("Expected to hit ChannelException, but did not!",
            source.hitChannelException());
    Assert.assertEquals(8, dataOut.size());
    source.stop();
  }




  @Test
  public void testReconfigure() throws InterruptedException, IOException {
    final int NUM_RECONFIGS = 20;
    Context context = new Context();
    String bucketName = "filesbucket";
    File bucketDir = new File(tmpDir, bucketName);
    if(bucketDir.exists()) {
      bucketDir.delete();
    }
    bucketDir.mkdir();
    for (int i = 0; i < NUM_RECONFIGS; i++) {
      File file = new File(bucketDir.getAbsolutePath() + "/file1");
      Files.write("File " + i, file, Charsets.UTF_8);
      context.put(S3SourceConfigurationConstants.BACKING_DIR, tmpDir.getAbsolutePath());
      context.put(S3SourceConfigurationConstants.ACCESS_KEY, "");
      context.put(S3SourceConfigurationConstants.SECRET_KEY, "");
      context.put(S3SourceConfigurationConstants.BUCKET_NAME, bucketName);
      //context.put(S3SourceConfigurationConstants.BATCH_SIZE, "2");
      Configurables.configure(source, context);
      source.setS3Client(new TestAmazonS3Client(tmpDir.getAbsolutePath()));
      source.start();
      Thread.sleep(TimeUnit.SECONDS.toMillis(1));
      Transaction txn = channel.getTransaction();
      txn.begin();
      try {
        Event event = channel.take();
        String content = new String(event.getBody(), Charsets.UTF_8);
        Assert.assertEquals("File " + i, content);
        txn.commit();
      } catch (Throwable t) {
        txn.rollback();
      } finally {
        txn.close();
      }
      source.stop();
      Assert.assertFalse("Fatal error on iteration " + i, source.hasFatalError());
    }
  }

  @Test
  public void testLifecycle() throws IOException, InterruptedException {
    Context context = new Context();
    String bucketName = "filesbucket";
    File bucketDir = new File(tmpDir, bucketName);
    if(bucketDir.exists()) {
      bucketDir.delete();
    }
    bucketDir.mkdir();
    File f1 = new File(bucketDir.getAbsolutePath() + "/file1");

    Files.write("file1line1\nfile1line2\nfile1line3\nfile1line4\n" +
                    "file1line5\nfile1line6\nfile1line7\nfile1line8\n",
            f1, Charsets.UTF_8);

    context.put(S3SourceConfigurationConstants.BACKING_DIR, tmpDir.getAbsolutePath());
    context.put(S3SourceConfigurationConstants.ACCESS_KEY, "");
    context.put(S3SourceConfigurationConstants.SECRET_KEY, "");
    context.put(S3SourceConfigurationConstants.BUCKET_NAME, bucketName);
    Configurables.configure(source, context);

    for (int i = 0; i < 10; i++) {
      source.setS3Client(new TestAmazonS3Client(tmpDir.getAbsolutePath()));
      source.start();

      Assert.assertTrue("Reached start or error", LifecycleController.waitForOneOf(
                      source, LifecycleState.START_OR_ERROR));
      Assert.assertEquals("Server is started", LifecycleState.START,
              source.getLifecycleState());

      source.stop();
      Assert.assertTrue("Reached stop or error",
              LifecycleController.waitForOneOf(source, LifecycleState.STOP_OR_ERROR));
      Assert.assertEquals("Server is stopped", LifecycleState.STOP,
              source.getLifecycleState());
    }
  }



  @Test
  public void testPutFilenameHeader() throws IOException, InterruptedException {
    Context context = new Context();
    String bucketName = "filesbucket";
    File bucketDir = new File(tmpDir, bucketName);
    if(bucketDir.exists()) {
      bucketDir.delete();
    }
    bucketDir.mkdir();
    File f1 = new File(bucketDir.getAbsolutePath() + "/file1");

    Files.write("file1line1\nfile1line2\nfile1line3\nfile1line4\n" +
                "file1line5\nfile1line6\nfile1line7\nfile1line8\n",
                f1, Charsets.UTF_8);

    context.put(S3SourceConfigurationConstants.BACKING_DIR, tmpDir.getAbsolutePath());
    context.put(S3SourceConfigurationConstants.ACCESS_KEY, "");
    context.put(S3SourceConfigurationConstants.SECRET_KEY, "");
    context.put(S3SourceConfigurationConstants.BUCKET_NAME, bucketName);
    Configurables.configure(source, context);

    source.setS3Client(new TestAmazonS3Client(tmpDir.getAbsolutePath()));
    source.start();
    while (source.getSourceCounter().getEventAcceptedCount() < 8) {
      Thread.sleep(10);
    }
    Transaction txn = channel.getTransaction();
    txn.begin();
    Event e = channel.take();
    Assert.assertNotNull("Event must not be null", e);

    txn.commit();
    txn.close();
  }

  @Test
  public void testPutBasenameHeader() throws IOException,
    InterruptedException {
    Context context = new Context();
    String bucketName = "filesbucket";
    File bucketDir = new File(tmpDir, bucketName);
    if(bucketDir.exists()) {
      bucketDir.delete();
    }
    bucketDir.mkdir();
    File f1 = new File(bucketDir.getAbsolutePath() + "/file1");

    Files.write("file1line1\nfile1line2\nfile1line3\nfile1line4\n" +
      "file1line5\nfile1line6\nfile1line7\nfile1line8\n",
      f1, Charsets.UTF_8);

    context.put(S3SourceConfigurationConstants.BACKING_DIR, tmpDir.getAbsolutePath());
    context.put(S3SourceConfigurationConstants.ACCESS_KEY, "");
    context.put(S3SourceConfigurationConstants.SECRET_KEY, "");
    context.put(S3SourceConfigurationConstants.BUCKET_NAME, bucketName);

    Configurables.configure(source, context);
    source.setS3Client(new TestAmazonS3Client(tmpDir.getAbsolutePath()));
    source.start();
    while (source.getSourceCounter().getEventAcceptedCount() < 8) {
      Thread.sleep(10);
    }
    Transaction txn = channel.getTransaction();
    txn.begin();
    Event e = channel.take();
    Assert.assertNotNull("Event must not be null", e);

    txn.commit();
    txn.close();
  }
}

class TestAmazonS3Client extends AmazonS3Client {

  String parentDir;

  TestAmazonS3Client(String parentDir) {
    this.parentDir = parentDir;
  }

  @Override
  public TestObjectListing listObjects(String bucketName) {
    File dir = new File(parentDir, bucketName);
    System.out.println("Test Obj Listing for Dir ----- "+ dir.getAbsolutePath());
    File[] files = dir.listFiles();
    return new TestObjectListing(files);
  }

  @Override
  public TestS3Object getObject(String bucketName, String  fileName) {
    File dir = new File(parentDir, bucketName);
    File[] files = dir.listFiles();
    for(File file: files) {
      if(file.getName().equals(fileName))
        return new TestS3Object(file);
    }
    return null;
  }
}

class TestObjectListing extends ObjectListing {

  File[] files;
  public TestObjectListing(File[] files) {
    this.files = files;
  }

  @Override
  public List<S3ObjectSummary> getObjectSummaries() {
    List<S3ObjectSummary> list = new ArrayList<S3ObjectSummary>();
    for(File file: files) {
      if(!file.getName().startsWith("."))
        list.add(new TestS3ObjectSummary(file));
    }
    return list;
  }

}

class TestS3ObjectSummary extends S3ObjectSummary {

  File file;
  public TestS3ObjectSummary(File file) {
    this.file = file;
  }

  @Override
  public String getKey() {
    return file.getName();
  }

  @Override
  public long getSize() {
    return file.length();
  }

}

class TestS3Object extends S3Object {
  File file;
  TestS3Object(File file) {
    this.file = file;
  }

  @Override
  public S3ObjectInputStream getObjectContent() {
    FileInputStream fis;
    TestS3ObjectInputStream Testfis = null;
    try {
      fis = new FileInputStream(file);
      Testfis = new TestS3ObjectInputStream(fis);
    } catch(FileNotFoundException ex) {
      ex.printStackTrace();
    }
    return Testfis;
  }
}

class TestS3ObjectInputStream extends S3ObjectInputStream {
  InputStream in;
  public TestS3ObjectInputStream(FileInputStream fis) throws FileNotFoundException{
    super(fis, new HttpRequestBase() {
      @Override
      public String getMethod() {
        return null;
      }
    });
    in = fis;
  }

  @Override public int available() throws IOException { return in.available(); }
  @Override public void close() throws IOException { in.close(); }
  @Override public void mark(int readLimit) { in.mark(readLimit); }
  @Override public boolean markSupported() { return in.markSupported(); }
  @Override public int read() throws IOException { return in.read(); }
  @Override public int read(byte[] b) throws IOException { return in.read(b); }
  @Override public int read(byte[] b, int off, int len) throws IOException { return in.read(b, off, len); }
  @Override public void reset() throws IOException { in.reset(); }
  @Override public long skip(long n) throws IOException { return in.skip(n); }

}
