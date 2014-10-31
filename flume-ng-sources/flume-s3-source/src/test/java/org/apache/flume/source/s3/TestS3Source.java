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

  @Test
  public void testS3SourceOneFile() throws Exception {

    channel.start();
    Context context = new Context();
    File f1 = new File(tmpDir.getAbsolutePath() + "/file1");

    Files.write("file1line1\nfile1line2\nfile1line3\nfile1line4\n" +
                    "file1line5\nfile1line6\nfile1line7\nfile1line8\n",
            f1, Charsets.UTF_8);

    System.out.println("------------- tmp Dir path is -------------------  " + tmpDir.getAbsolutePath());
    context.put(S3SourceConfigurationConstants.BACKING_DIR, tmpDir.getAbsolutePath());
//    context.put(S3SourceConfigurationConstants.ACCESS_KEY, "AKIAIACMXFTEW2G2JLMQ");
//    context.put(S3SourceConfigurationConstants.SECRET_KEY, "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");
//    context.put(S3SourceConfigurationConstants.BUCKET_NAME, "jrufusbucket1");
    context.put(S3SourceConfigurationConstants.ACCESS_KEY, "");
    context.put(S3SourceConfigurationConstants.SECRET_KEY, "");
    context.put(S3SourceConfigurationConstants.BUCKET_NAME, "conf");
    context.put(S3SourceConfigurationConstants.BATCH_SIZE, "5");

    Configurables.configure(source, context);
    source.setBackOff(false);

    File file = new File("/Users/jrufus/dev");
    source.setS3Client(new TestAmazonS3Client());
    source.start();

    List<String> dataOut = Lists.newArrayList();
  int i = 0;
  while(true) {
    System.out.println("------------- i -------------------  " + i);
    Transaction tx = channel.getTransaction();
    tx.begin();
    Event e = channel.take();
    if (e != null) {
      dataOut.add(new String(e.getBody(), "UTF-8"));
      i++;
      System.out.println(new String(e.getBody(), "UTF-8"));
    }

    tx.commit();
    tx.close();
//    if(e == null) {
//      System.out.println("------------- BREAKING -------------------  " + i);
//      break;
//    }
//    if(e == null && i > 50)
//      break;
//    else if(e == null){
//      System.out.println("ABOUT TO SLEEP - -------------------");
//      Thread.sleep(30000);
//    }
  }
//  Assert.assertTrue("Expected to hit ChannelException, but did not!",
//            source.hitChannelException());
//  Assert.assertEquals(8, dataOut.size());
    //source.stop();
  }


//
//  //@Test
//  public void testPutFilenameHeader() throws IOException, InterruptedException {
//    Context context = new Context();
//    File f1 = new File(tmpDir.getAbsolutePath() + "/file1");
//
//    Files.write("file1line1\nfile1line2\nfile1line3\nfile1line4\n" +
//                "file1line5\nfile1line6\nfile1line7\nfile1line8\n",
//                f1, Charsets.UTF_8);
//
//    context.put(SpoolDirectorySourceConfigurationConstants.SPOOL_DIRECTORY,
//        tmpDir.getAbsolutePath());
//    context.put(SpoolDirectorySourceConfigurationConstants.FILENAME_HEADER,
//        "true");
//    context.put(SpoolDirectorySourceConfigurationConstants.FILENAME_HEADER_KEY,
//        "fileHeaderKeyTest");
//
//    Configurables.configure(source, context);
//    source.start();
//    while (source.getSourceCounter().getEventAcceptedCount() < 8) {
//      Thread.sleep(10);
//    }
//    Transaction txn = channel.getTransaction();
//    txn.begin();
//    Event e = channel.take();
//    Assert.assertNotNull("Event must not be null", e);
//    Assert.assertNotNull("Event headers must not be null", e.getHeaders());
//    Assert.assertNotNull(e.getHeaders().get("fileHeaderKeyTest"));
//    Assert.assertEquals(f1.getAbsolutePath(),
//        e.getHeaders().get("fileHeaderKeyTest"));
//    txn.commit();
//    txn.close();
//  }
//
//  //@Test
//  public void testPutBasenameHeader() throws IOException,
//    InterruptedException {
//    Context context = new Context();
//    File f1 = new File(tmpDir.getAbsolutePath() + "/file1");
//
//    Files.write("file1line1\nfile1line2\nfile1line3\nfile1line4\n" +
//      "file1line5\nfile1line6\nfile1line7\nfile1line8\n",
//      f1, Charsets.UTF_8);
//
//    context.put(SpoolDirectorySourceConfigurationConstants.SPOOL_DIRECTORY,
//        tmpDir.getAbsolutePath());
//    context.put(SpoolDirectorySourceConfigurationConstants.BASENAME_HEADER,
//        "true");
//    context.put(SpoolDirectorySourceConfigurationConstants.BASENAME_HEADER_KEY,
//        "basenameHeaderKeyTest");
//
//    Configurables.configure(source, context);
//    source.start();
//    while (source.getSourceCounter().getEventAcceptedCount() < 8) {
//      Thread.sleep(10);
//    }
//    Transaction txn = channel.getTransaction();
//    txn.begin();
//    Event e = channel.take();
//    Assert.assertNotNull("Event must not be null", e);
//    Assert.assertNotNull("Event headers must not be null", e.getHeaders());
//    Assert.assertNotNull(e.getHeaders().get("basenameHeaderKeyTest"));
//    Assert.assertEquals(f1.getName(),
//      e.getHeaders().get("basenameHeaderKeyTest"));
//    txn.commit();
//    txn.close();
//  }
//
//  //@Test
//  public void testLifecycle() throws IOException, InterruptedException {
//    Context context = new Context();
//    File f1 = new File(tmpDir.getAbsolutePath() + "/file1");
//
//    Files.write("file1line1\nfile1line2\nfile1line3\nfile1line4\n" +
//                "file1line5\nfile1line6\nfile1line7\nfile1line8\n",
//                f1, Charsets.UTF_8);
//
//    context.put(SpoolDirectorySourceConfigurationConstants.SPOOL_DIRECTORY,
//        tmpDir.getAbsolutePath());
//
//    Configurables.configure(source, context);
//
//    for (int i = 0; i < 10; i++) {
//      source.start();
//
//      Assert
//          .assertTrue("Reached start or error", LifecycleController.waitForOneOf(
//              source, LifecycleState.START_OR_ERROR));
//      Assert.assertEquals("Server is started", LifecycleState.START,
//          source.getLifecycleState());
//
//      source.stop();
//      Assert.assertTrue("Reached stop or error",
//          LifecycleController.waitForOneOf(source, LifecycleState.STOP_OR_ERROR));
//      Assert.assertEquals("Server is stopped", LifecycleState.STOP,
//          source.getLifecycleState());
//    }
//  }
//
//  //@Test
//  public void testReconfigure() throws InterruptedException, IOException {
//    final int NUM_RECONFIGS = 20;
//    for (int i = 0; i < NUM_RECONFIGS; i++) {
//      Context context = new Context();
//      File file = new File(tmpDir.getAbsolutePath() + "/file-" + i);
//      Files.write("File " + i, file, Charsets.UTF_8);
//      context.put(SpoolDirectorySourceConfigurationConstants.SPOOL_DIRECTORY,
//          tmpDir.getAbsolutePath());
//      Configurables.configure(source, context);
//      source.start();
//      Thread.sleep(TimeUnit.SECONDS.toMillis(1));
//      Transaction txn = channel.getTransaction();
//      txn.begin();
//      try {
//        Event event = channel.take();
//        String content = new String(event.getBody(), Charsets.UTF_8);
//        Assert.assertEquals("File " + i, content);
//        txn.commit();
//      } catch (Throwable t) {
//        txn.rollback();
//      } finally {
//        txn.close();
//      }
//      source.stop();
//      Assert.assertFalse("Fatal error on iteration " + i, source.hasFatalError());
//    }
//  }
//
//  //@Test
//  public void testSourceDoesNotDieOnFullChannel() throws Exception {
//
//    Context chContext = new Context();
//    chContext.put("capacity", "2");
//    chContext.put("transactionCapacity", "2");
//    chContext.put("keep-alive", "0");
//    channel.stop();
//    Configurables.configure(channel, chContext);
//
//    channel.start();
//    Context context = new Context();
//    File f1 = new File(tmpDir.getAbsolutePath() + "/file1");
//
//    Files.write("file1line1\nfile1line2\nfile1line3\nfile1line4\n" +
//      "file1line5\nfile1line6\nfile1line7\nfile1line8\n",
//      f1, Charsets.UTF_8);
//
//
//    context.put(SpoolDirectorySourceConfigurationConstants.SPOOL_DIRECTORY,
//      tmpDir.getAbsolutePath());
//
//    context.put(SpoolDirectorySourceConfigurationConstants.BATCH_SIZE, "2");
//    Configurables.configure(source, context);
//    source.setBackOff(false);
//    source.start();
//
//    // Wait for the source to read enough events to fill up the channel.
//    while(!source.hitChannelException()) {
//      Thread.sleep(50);
//    }
//
//    List<String> dataOut = Lists.newArrayList();
//
//    for (int i = 0; i < 8; ) {
//      Transaction tx = channel.getTransaction();
//      tx.begin();
//      Event e = channel.take();
//      if (e != null) {
//        dataOut.add(new String(e.getBody(), "UTF-8"));
//        i++;
//      }
//      e = channel.take();
//      if (e != null) {
//        dataOut.add(new String(e.getBody(), "UTF-8"));
//        i++;
//      }
//      tx.commit();
//      tx.close();
//    }
//    Assert.assertTrue("Expected to hit ChannelException, but did not!",
//      source.hitChannelException());
//    Assert.assertEquals(8, dataOut.size());
//    source.stop();
//  }
}

class TestAmazonS3Client extends AmazonS3Client {

  @Override
  public TestObjectListing listObjects(String dirName) {
    File dir = new File(new File("/Users/jrufus/dev/"), dirName);
    File[] files = dir.listFiles();
    return new TestObjectListing(files);
  }

  @Override
  public TestS3Object getObject(String dirName, String  fileName) {
    File dir = new File(new File("/Users/jrufus/dev/"), dirName);
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
