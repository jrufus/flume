/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flume.source.s3;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.util.StringUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import org.apache.flume.*;
import org.apache.flume.client.avro.ReliableSpoolingFileEventReader;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.serialization.DecodeErrorPolicy;
import org.apache.flume.source.AbstractSource;
import org.apache.flume.source.SpoolDirectorySourceConfigurationConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


import org.mapdb.DB;
import org.mapdb.DBMaker;

import static org.apache.flume.source.s3.S3SourceConfigurationConstants.*;


/**
 * A Source for Amazon S3.
 *
 */
public class S3Source extends AbstractSource
        implements Configurable, EventDrivenSource {

  private static final Logger logger = LoggerFactory.getLogger(S3Source.class);
// Delay used when polling for new files
  private static final int POLL_DELAY_MS = 500;

  /* Config options */
  private String bucketName;
  private int batchSize;
  private String accessKey;
  private String secretKey;
  private String endPoint;
  private String backingDir;
  private MetadataBackingStore backingStore;

  private String deserializerType;
  private Context deserializerContext;
  private String inputCharset;
  private DecodeErrorPolicy decodeErrorPolicy;
  private volatile boolean hasFatalError = false;

  private ScheduledExecutorService executor;
  private boolean backoff = true;
  private boolean hitChannelException = false;
  private int maxBackoff;
  private SourceCounter sourceCounter;
  private S3ObjectEventReader reader;
  private  AmazonS3Client s3Client;

  @Override
  public synchronized void configure(Context context) {

    bucketName = context.getString(BUCKET_NAME);
    Preconditions.checkState(bucketName != null, "Configuration must specify a bucket Name");

    accessKey = context.getString("ACCESS_KEY");
    Preconditions.checkState(bucketName != null,  "Configuration must specify an access key Id");

    secretKey = context.getString(SECRET_KEY);
    Preconditions.checkState(bucketName != null, "Configuration must specify a secret key");

    backingDir = context.getString(BACKING_DIR);
    Preconditions.checkState(backingDir != null, "Configuration must specify a directory for storing metadata");

    batchSize = context.getInteger(BATCH_SIZE, DEFAULT_BATCH_SIZE);

    int capacity = 0;
    String name = "abc";
    String storeType = "Memory";
    backingStore = MetadataBackingStoreFactory.get(storeType, capacity, name, context.getString(BACKING_STORE, DEFAULT_BACKING_STORE));

    deserializerType = context.getString(DESERIALIZER, DEFAULT_DESERIALIZER);
    deserializerContext = new Context(context.getSubProperties(DESERIALIZER + "."));

    inputCharset = context.getString(INPUT_CHARSET, DEFAULT_INPUT_CHARSET);
    decodeErrorPolicy = DecodeErrorPolicy.valueOf(
                        context.getString(DECODE_ERROR_POLICY, DEFAULT_DECODE_ERROR_POLICY)
                        .toUpperCase(Locale.ENGLISH));
    maxBackoff = context.getInteger(MAX_BACKOFF, DEFAULT_MAX_BACKOFF);
    if (sourceCounter == null) {
      sourceCounter = new SourceCounter(getName());
    }

    AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey );
    s3Client = new AmazonS3Client(credentials);
    if(endPoint != null) {
      s3Client.setEndpoint(endPoint);
    }

  }

  @Override
  public synchronized void start() {
    logger.info("S3Source source starting with bucket - ", bucketName);

    executor = Executors.newSingleThreadScheduledExecutor();

    File directory = new File(backingDir);
    try {

      reader = new S3ObjectEventReader.Builder()
              .backingDirectory(directory)
              .deserializerType(deserializerType)
              .deserializerContext(deserializerContext)
              .inputCharset(inputCharset)
              .decodeErrorPolicy(decodeErrorPolicy)
              .setS3Client(s3Client)
              .build();
    } catch (IOException ioe) {
      throw new FlumeException("Error instantiating spooling event parser",
              ioe);
    }

    Runnable runner = new S3SourceRunnable(reader, sourceCounter);
    executor.scheduleWithFixedDelay(
            runner, 0, POLL_DELAY_MS, TimeUnit.MILLISECONDS);

    super.start();
    logger.debug("SpoolDirectorySource source started");
    sourceCounter.start();
  }

  @Override
  public synchronized void stop() {
    executor.shutdown();
    try {
      executor.awaitTermination(10L, TimeUnit.SECONDS);
    } catch (InterruptedException ex) {
      logger.info("Interrupted while awaiting termination", ex);
    }
    executor.shutdownNow();

    super.stop();
    sourceCounter.stop();
    logger.info("SpoolDir source {} stopped. Metrics: {}", getName(),
            sourceCounter);
  }

  @Override
  public String toString() {
    return "S3 Source  " + getName() +
            ": { bucket Name: " + bucketName + " }";
  }

  @VisibleForTesting
  protected boolean hasFatalError() {
    return hasFatalError;
  }



  /**
   * The class always backs off, this exists only so that we can test without
   * taking a really long time.
   * @param backoff - whether the source should backoff if the channel is full
   */
  @VisibleForTesting
  protected void setBackOff(boolean backoff) {
    this.backoff = backoff;
  }

  @VisibleForTesting
  protected boolean hitChannelException() {
    return hitChannelException;
  }

  @VisibleForTesting
  protected SourceCounter getSourceCounter() {
    return sourceCounter;
  }

  private class S3SourceRunnable implements Runnable {
    private S3ObjectEventReader reader;
    private SourceCounter sourceCounter;

    public S3SourceRunnable(S3ObjectEventReader reader,
                                  SourceCounter sourceCounter) {
      this.reader = reader;
      this.sourceCounter = sourceCounter;
    }

    @Override
    public void run() {
      int backoffInterval = 250;
      try {
        while (!Thread.interrupted()) {
          List<Event> events = reader.readEvents(batchSize);
          if (events.isEmpty()) {
            break;
          }
          sourceCounter.addToEventReceivedCount(events.size());
          sourceCounter.incrementAppendBatchReceivedCount();

          try {
            getChannelProcessor().processEventBatch(events);
            reader.commit();
          } catch (ChannelException ex) {
            logger.warn("The channel is full, and cannot write data now. The " +
                    "source will try again after " + String.valueOf(backoffInterval) +
                    " milliseconds");
            hitChannelException = true;
            if (backoff) {
              TimeUnit.MILLISECONDS.sleep(backoffInterval);
              backoffInterval = backoffInterval << 1;
              backoffInterval = backoffInterval >= maxBackoff ? maxBackoff :
                      backoffInterval;
            }
            continue;
          }
          backoffInterval = 250;
          sourceCounter.addToEventAcceptedCount(events.size());
          sourceCounter.incrementAppendBatchAcceptedCount();
        }
        logger.info("Spooling Directory Source runner has shutdown.");
      } catch (Throwable t) {
        logger.error("FATAL: " + S3Source.this.toString() + ": " +
                "Uncaught exception in SpoolDirectorySource thread. " +
                "Restart or reconfigure Flume to continue processing.", t);
        hasFatalError = true;
        Throwables.propagate(t);
      }
    }
  }

  public void connectToS3() {
    AWSCredentials credentials = new BasicAWSCredentials("AKIAIACMXFTEW2G2JLMQ", "dlE3nACto1tA+3V2m92vXhHxvwCeZZVsTHgAprqn");
    AmazonS3 conn = new AmazonS3Client(credentials);
    if(endPoint != null) {
      conn.setEndpoint(endPoint);
    }
    String bucketName = "jrufusbucket1";
    ObjectListing listing = conn.listObjects(bucketName);
    List<S3ObjectSummary> summaries = listing.getObjectSummaries();
    for(S3ObjectSummary summary : summaries) {
      String key = summary.getKey();
      System.out.println(key);
      S3Object object = conn.getObject(bucketName, summary.getKey());
      S3ObjectInputStream is = object.getObjectContent();
      BufferedInputStream bis = new BufferedInputStream(is);
      System.out.println("---- mark supported-----" + bis.markSupported());
    }
  }

  public static void main(String[] args) {
    new S3Source().connectToS3();
  }
}