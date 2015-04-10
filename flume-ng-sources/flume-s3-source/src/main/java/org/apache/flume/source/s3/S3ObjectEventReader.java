/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flume.source.s3;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.io.Files;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.client.avro.ReliableEventReader;
import org.apache.flume.serialization.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;

/**
 * <p/>A {@link org.apache.flume.source.s3.S3ObjectEventReader} which reads log data from files stored
 * in a S3 Bucket and reads (through {@link org.apache.flume.serialization.EventDeserializer#readEvent()} calls).
 * The user must {@link #commit()} each read, to indicate that the lines have been fully
 * processed.  <p/>Read calls will return no data if there are no files left to read. This
 * class, in general, is not thread safe.
 *
 * <p/>This reader assumes that files with unique file names are left in the
 * S3 Bucket and not modified once they are placed there. Any user
 * behavior which violates these assumptions, when detected, will result in a
 * FlumeException being thrown.
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class S3ObjectEventReader implements ReliableEventReader {

  private static final Logger logger = LoggerFactory
      .getLogger(S3ObjectEventReader.class);


  private final String bucketName;
  private final String deserializerType;
  private final Context deserializerContext;
  private final Charset inputCharset;
  private final DecodeErrorPolicy decodeErrorPolicy;

  private Optional<S3ObjectInfo> currentFile = Optional.absent();
  /** Always contains the last file from which lines have been read. **/
  private Optional<S3ObjectInfo> lastFileRead = Optional.absent();
  private boolean committed = true;

  private int listFilesCount = 0;
  private ObjectListing objListing;
  private ListIterator<S3ObjectSummary> objIter;
  private AmazonS3Client s3Client;
  private MetadataBackingStore backingStore;

  /**
   * Create a S3ObjectEventReader to watch the given directory.
   */
  private S3ObjectEventReader(String bucketName,
                String deserializerType,Context deserializerContext,
                String inputCharset, DecodeErrorPolicy decodeErrorPolicy,
                AmazonS3Client s3Client, MetadataBackingStore backingStore)
          throws IOException {
    // Sanity checks
    Preconditions.checkNotNull(deserializerType);
    Preconditions.checkNotNull(deserializerContext);
    Preconditions.checkNotNull(inputCharset);
    Preconditions.checkNotNull(s3Client);
    Preconditions.checkNotNull(backingStore);


    if (logger.isDebugEnabled()) {
      logger.debug("Initializing {} with directory={}, metaDataStoreType={}, " +
          "deserializer={}",
          new Object[] { S3ObjectEventReader.class.getSimpleName(),
                  bucketName, backingStore.getName(), deserializerType });
    }

    this.bucketName = bucketName;
    this.deserializerType = deserializerType;
    this.deserializerContext = deserializerContext;

    this.inputCharset = Charset.forName(inputCharset);
    this.decodeErrorPolicy = Preconditions.checkNotNull(decodeErrorPolicy);

    this.s3Client = s3Client;
    this.backingStore = backingStore;


  }

  @VisibleForTesting
  int getListFilesCount() {
    return listFilesCount;
  }

  /** Return the filename which generated the data from the last successful
   * {@link #readEvents(int)} call. Returns null if called before any file
   * contents are read. */
  public String getLastFileRead() {
    if (!lastFileRead.isPresent()) {
      return null;
    }
    return lastFileRead.get().getKey();
  }

  // public interface
  public Event readEvent() throws IOException {
    List<Event> events = readEvents(1);
    if (!events.isEmpty()) {
      return events.get(0);
    } else {
      return null;
    }
  }

  public List<Event> readEvents(int numEvents) throws IOException {
    if (!committed) {
      if (!currentFile.isPresent()) {
        throw new IllegalStateException("File should exist when " +
            "commit is outstanding.");
      }
      logger.info("Last read was never committed - resetting mark position.");
      currentFile.get().getDeserializer().reset();
    } else {
      // Check if new files have arrived since last call
      if (!currentFile.isPresent()) {
        currentFile = getNextFile();
      }
      // Return empty list if no new files
      if (!currentFile.isPresent()) {
        return Collections.emptyList();
      }
    }

    EventDeserializer des = currentFile.get().getDeserializer();
    List<Event> events = des.readEvents(numEvents);

    /* It's possible that the last read took us just up to a file boundary.
     * If so, try to roll to the next file, if there is one. */
    if (events.isEmpty()) {
      retireCurrentFile();
      currentFile = getNextFile();
      if (!currentFile.isPresent()) {
        return Collections.emptyList();
      }
      events = currentFile.get().getDeserializer().readEvents(numEvents);
    }

    committed = false;
    lastFileRead = currentFile;
    return events;
  }

  @Override
  public void close() throws IOException {
    if (currentFile.isPresent()) {
      currentFile.get().getDeserializer().close();
      currentFile = Optional.absent();
    }
  }

  /** Commit the last lines which were read. */
  public void commit() throws IOException {
    if (!committed && currentFile.isPresent()) {
      currentFile.get().getDeserializer().mark();
      committed = true;
    }
  }

  /**
   * Closes currentFile
   *
   * If these operations fail in a way that may cause duplicate log entries,
   * an error is logged but no exceptions are thrown.
   */
  private void retireCurrentFile() throws IOException {
    Preconditions.checkState(currentFile.isPresent());
    String key = currentFile.get().getKey();
    currentFile.get().getDeserializer().close();
    backingStore.add(key);
  }

  /**
   * Returns the next file to be consumed from the s3 Bucket
   * If the bucket is empty or the chosen file is not readable,
   * this will return an absent option
   */
  private Optional<S3ObjectInfo> getNextFile() {
    if(objListing == null) {
      logger.debug("Getting new listing");
      objListing = s3Client.listObjects(bucketName);
      objIter = null;
    }
    if(objIter == null) {
      objIter = objListing.getObjectSummaries().listIterator();
    }
    if(objIter.hasNext()) {
      S3ObjectSummary objSummary = objIter.next();
      if(backingStore.contains(objSummary.getKey())) {
        return getNextFile();
      } else {
        return openFile(objSummary);
      }
    } else if(objListing.isTruncated()) {
      objListing = s3Client.listNextBatchOfObjects(objListing);
      objIter = null;
      return getNextFile();
    } else {
      objListing = null;
      return Optional.absent();
    }
  }


  /**
   * Opens a file for consuming
   * @param
   * @return {@link } for the file to consume or absent option if the
   * file does not exists or readable.
   */
  private Optional<S3ObjectInfo> openFile(S3ObjectSummary objSummary) {

    S3Object object = s3Client.getObject(bucketName, objSummary.getKey());
    S3ObjectInputStream is = object.getObjectContent();
    String key = objSummary.getKey();
    try {
      // roll the meta file, if needed
      PositionTracker tracker = backingStore.getPositionTracker(key);

      if (!tracker.getTarget().equals(key)) {
        tracker.close();
        logger.debug("Deleting Meta file");
        backingStore.resetPositionTracker();
        tracker = backingStore.getPositionTracker(key);
      }

      // sanity check
      Preconditions.checkState(tracker.getTarget().equals(key),
          "Tracker target %s does not equal expected filename %s",
          tracker.getTarget(), key);

      ResettableInputStream in =
          new ResettableGenericInputStream(
                  new S3StreamCreator(s3Client, bucketName, key), tracker,
                  ResettableGenericInputStream.DEFAULT_BUF_SIZE, inputCharset,
                  decodeErrorPolicy, objSummary.getSize());
      EventDeserializer deserializer = EventDeserializerFactory.getInstance
          (deserializerType, deserializerContext, in);

      return Optional.of(new S3ObjectInfo(key, deserializer, objSummary.getSize()));
    } catch (FileNotFoundException e) {
      // File could have been deleted in the interim
      logger.warn("Could not find file: " + key, e);
      return Optional.absent();
    } catch (IOException e) {
      logger.error("Exception opening file: " + key, e);
      return Optional.absent();
    }
  }

  /** An immutable class with information about a S3Object being processed. */
  private static class S3ObjectInfo {
    private final String key;
    private final long length;
    private final EventDeserializer deserializer;

    public S3ObjectInfo(String key, EventDeserializer deserializer, long length) {
      this.key = key;
      this.length = length;
      this.deserializer = deserializer;
    }

    public long getLength() { return length; }
    public EventDeserializer getDeserializer() { return deserializer; }
    public String getKey() { return key; }
  }


  /**
   * Special builder class for S3ObjectEventReader
   */
  public static class Builder {
    //private File backingDirectory;
    private String bucketName;
    private String deserializerType =
            S3SourceConfigurationConstants.DEFAULT_DESERIALIZER;
    private Context deserializerContext = new Context();
    private String inputCharset =
            S3SourceConfigurationConstants.DEFAULT_INPUT_CHARSET;
    private DecodeErrorPolicy decodeErrorPolicy = DecodeErrorPolicy.valueOf(
            S3SourceConfigurationConstants.DEFAULT_DECODE_ERROR_POLICY
            .toUpperCase(Locale.ENGLISH));
    private AmazonS3Client s3Client;
    private MetadataBackingStore backingStore;

    public Builder setS3Client(AmazonS3Client s3Client) {
      this.s3Client = s3Client;
      return this;
    }

//    public Builder backingDirectory(File directory) {
//      this.backingDirectory = directory;
//      return this;
//    }

    public Builder bucket(String bucketName) {
      this.bucketName = bucketName;
      return this;
    }

    public Builder deserializerType(String deserializerType) {
      this.deserializerType = deserializerType;
      return this;
    }

    public Builder deserializerContext(Context deserializerContext) {
      this.deserializerContext = deserializerContext;
      return this;
    }

    public Builder inputCharset(String inputCharset) {
      this.inputCharset = inputCharset;
      return this;
    }

    public Builder decodeErrorPolicy(DecodeErrorPolicy decodeErrorPolicy) {
      this.decodeErrorPolicy = decodeErrorPolicy;
      return this;
    }

    public Builder backingStore(MetadataBackingStore backingStore) {
      this.backingStore = backingStore;
      return this;
    }

    public S3ObjectEventReader build() throws IOException {
      return new S3ObjectEventReader(bucketName,
              deserializerType, deserializerContext, inputCharset,
              decodeErrorPolicy, s3Client, backingStore);
    }
  }

}
