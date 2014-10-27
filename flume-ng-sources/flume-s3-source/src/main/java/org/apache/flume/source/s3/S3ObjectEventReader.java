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
import org.apache.flume.serialization.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;

/**
 * <p/>A {@link org.apache.flume.client.avro.ReliableEventReader} which reads log data from files stored
 * in a spooling directory and renames each file once all of its data has been
 * read (through {@link org.apache.flume.serialization.EventDeserializer#readEvent()} calls). The user must
 * {@link #commit()} each read, to indicate that the lines have been fully
 * processed.
 * <p/>Read calls will return no data if there are no files left to read. This
 * class, in general, is not thread safe.
 *
 * <p/>This reader assumes that files with unique file names are left in the
 * spooling directory and not modified once they are placed there. Any user
 * behavior which violates these assumptions, when detected, will result in a
 * FlumeException being thrown.
 *
 * <p/>This class makes the following guarantees, if above assumptions are met:
 * <ul>
 * <li> Once a log file has been renamed with the completedSuffix,
 *      all of its records have been read through the
 *      {@link org.apache.flume.serialization.EventDeserializer#readEvent()} function and
 *      {@link #commit()}ed at least once.
 * <li> All files in the spooling directory will eventually be opened
 *      and delivered to a {@link #readEvents(int)} caller.
 * </ul>
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class S3ObjectEventReader  {

  private static final Logger logger = LoggerFactory
      .getLogger(S3ObjectEventReader.class);

  static final String metaFileName = ".flumes3-main.meta";

  private final File backingDirectory;
  private final String bucketName;
  private final String deserializerType;
  private final Context deserializerContext;
  private final File metaFile;
  private final Charset inputCharset;
  private final DecodeErrorPolicy decodeErrorPolicy;

  private Optional<S3ObjectInfo> currentFile = Optional.absent();
  /** Always contains the last file from which lines have been read. **/
  private Optional<S3ObjectInfo> lastFileRead = Optional.absent();
  private boolean committed = true;

  /** Instance var to Cache directory listing **/
  private Iterator<File> candidateFileIter = null;
  private int listFilesCount = 0;
  private ObjectListing objListing;
  private ListIterator<S3ObjectSummary> objIter;
  private AmazonS3Client s3Client;

  /**
   * Create a S3ObjectEventReader to watch the given directory.
   */
  private S3ObjectEventReader(File backingDirectory, String bucketName,
                              String deserializerType,Context deserializerContext,
                              String inputCharset, DecodeErrorPolicy decodeErrorPolicy,
                              AmazonS3Client s3Client) throws IOException {
    // Sanity checks
    Preconditions.checkNotNull(backingDirectory);
    Preconditions.checkNotNull(deserializerType);
    Preconditions.checkNotNull(deserializerContext);
    Preconditions.checkNotNull(inputCharset);
    Preconditions.checkNotNull(s3Client);

    if (logger.isDebugEnabled()) {
      logger.debug("Initializing {} with directory={}, metaDir={}, " +
          "deserializer={}",
          new Object[] { S3ObjectEventReader.class.getSimpleName(),
                  bucketName, backingDirectory, deserializerType });
    }

    // Verify directory exists and is readable/writable
    Preconditions.checkState(backingDirectory.exists(),
        "Directory does not exist: " + backingDirectory.getAbsolutePath());
    Preconditions.checkState(backingDirectory.isDirectory(),
        "Path is not a directory: " + backingDirectory.getAbsolutePath());

    // Do a canary test to make sure we have access to spooling directory
    try {
      File canary = File.createTempFile("flume-spooldir-perm-check-", ".canary", backingDirectory);
      Files.write("testing flume file permissions\n", canary, Charsets.UTF_8);
      List<String> lines = Files.readLines(canary, Charsets.UTF_8);
      Preconditions.checkState(!lines.isEmpty(), "Empty canary file %s", canary);
      if (!canary.delete()) {
        throw new IOException("Unable to delete canary file " + canary);
      }
      logger.debug("Successfully created and deleted canary file: {}", canary);
    } catch (IOException e) {
      throw new FlumeException("Unable to read and modify files" +
          " in the spooling directory: " + backingDirectory, e);
    }

    this.backingDirectory = backingDirectory;
    this.bucketName = bucketName;
    this.deserializerType = deserializerType;
    this.deserializerContext = deserializerContext;

    this.inputCharset = Charset.forName(inputCharset);
    this.decodeErrorPolicy = Preconditions.checkNotNull(decodeErrorPolicy);

    this.s3Client = s3Client;

    File trackerDirectory = new File(backingDirectory, "tracker");

    // ensure that meta directory exists
    if (!trackerDirectory.exists()) {
      if (!trackerDirectory.mkdir()) {
        throw new IOException("Unable to mkdir nonexistent meta directory " + trackerDirectory);
      }
    }

    // ensure that the meta directory is a directory
    if (!trackerDirectory.isDirectory()) {
      throw new IOException("Specified meta directory is not a directory" + trackerDirectory);
    }

    this.metaFile = new File(trackerDirectory, metaFileName);
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
    return lastFileRead.get().getFile().getAbsolutePath();
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
        throw new IllegalStateException("File should not roll when " +
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
   * Closes currentFile and attempt to rename it.
   *
   * If these operations fail in a way that may cause duplicate log entries,
   * an error is logged but no exceptions are thrown. If these operations fail
   * in a way that indicates potential misuse of the spooling directory, a
   * FlumeException will be thrown.
   * @throws org.apache.flume.FlumeException if files do not conform to spooling assumptions
   */
  private void retireCurrentFile() throws IOException {
    Preconditions.checkState(currentFile.isPresent());

    File fileToRoll = new File(currentFile.get().getFile().getAbsolutePath());

    currentFile.get().getDeserializer().close();

    // Verify that spooling assumptions hold
    if (fileToRoll.lastModified() != currentFile.get().getLastModified()) {
      String message = "File has been modified since being read: " + fileToRoll;
      throw new IllegalStateException(message);
    }
    if (fileToRoll.length() != currentFile.get().getLength()) {
      String message = "File has changed size since being read: " + fileToRoll;
      throw new IllegalStateException(message);
    }

    //
    deleteCurrentFile(fileToRoll);
    /*if (deletePolicy.equalsIgnoreCase(DeletePolicy.NEVER.name())) {
      rollCurrentFile(fileToRoll);
    } else if (deletePolicy.equalsIgnoreCase(DeletePolicy.IMMEDIATE.name())) {
      deleteCurrentFile(fileToRoll);
    } else {
      // TODO: implement delay in the future
      throw new IllegalArgumentException("Unsupported delete policy: " +
          deletePolicy);
    }*/
  }


  /**
   * Delete the given spooled file
   * @param fileToDelete
   * @throws java.io.IOException
   */
  private void deleteCurrentFile(File fileToDelete) throws IOException {
    logger.info("Preparing to delete file {}", fileToDelete);
    if (!fileToDelete.exists()) {
      logger.warn("Unable to delete nonexistent file: {}", fileToDelete);
      return;
    }
    if (!fileToDelete.delete()) {
      throw new IOException("Unable to delete spool file: " + fileToDelete);
    }
    // now we no longer need the meta file
    deleteMetaFile();
  }

  /**
   * Returns the next file to be consumed from the chosen directory.
   * If the directory is empty or the chosen file is not readable,
   * this will return an absent option
   */
  private Optional<FileInfo> getNextFile() {
    if(objListing == null) {
      objListing = s3Client.listObjects(bucketName);
    }
    if(objIter == null) {
      objIter = objListing.getObjectSummaries().listIterator();
    }
    if(objIter.hasNext()) {
      //TODO : 1. get the object, check if its not processed and return it or call getNextFile()
      S3ObjectSummary objSummary = objIter.next();
      //TODO check if its not processed against MapDb
      //if(yet to process)
        // return openFile(objSummary)
      //else
           //return getNextFile();
    } else if(objListing.isTruncated()) {
      objListing = s3Client.listNextBatchOfObjects(objListing);
      objIter = null;
      return getNextFile();
    } else {
      return Optional.absent();
    }



//    List<File> candidateFiles = Collections.emptyList();
//
//      candidateFiles = Arrays.asList(spoolDirectory.listFiles(filter));
//      listFilesCount++;
//      candidateFileIter = candidateFiles.iterator();
//    }
//
//    if (!candidateFileIter.hasNext()) { // No matching file in spooling directory.
//      return Optional.absent();
//    }
//
//    File selectedFile = candidateFileIter.next();
//    if (consumeOrder == ConsumeOrder.RANDOM) { // Selected file is random.
//      return openFile(selectedFile);
//    } else if (consumeOrder == ConsumeOrder.YOUNGEST) {
//      for (File candidateFile: candidateFiles) {
//        long compare = selectedFile.lastModified() -
//            candidateFile.lastModified();
//        if (compare == 0) { // ts is same pick smallest lexicographically.
//          selectedFile = smallerLexicographical(selectedFile, candidateFile);
//        } else if (compare < 0) { // candidate is younger (cand-ts > selec-ts)
//          selectedFile = candidateFile;
//        }
//      }
//    } else { // default order is OLDEST
//      for (File candidateFile: candidateFiles) {
//        long compare = selectedFile.lastModified() -
//            candidateFile.lastModified();
//        if (compare == 0) { // ts is same pick smallest lexicographically.
//          selectedFile = smallerLexicographical(selectedFile, candidateFile);
//        } else if (compare > 0) { // candidate is older (cand-ts < selec-ts).
//          selectedFile = candidateFile;
//        }
//      }
//    }
//
//    return openFile(selectedFile);
    return null;
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
   try {
      // roll the meta file, if needed
      String nextPath = objSummary.getKey();
      PositionTracker tracker =
          DurablePositionTracker.getInstance(metaFile, nextPath);
      if (!tracker.getTarget().equals(nextPath)) {
        tracker.close();
        deleteMetaFile();
        tracker = DurablePositionTracker.getInstance(metaFile, nextPath);
      }

      // sanity check
      Preconditions.checkState(tracker.getTarget().equals(nextPath),
          "Tracker target %s does not equal expected filename %s",
          tracker.getTarget(), nextPath);

      ResettableInputStream in =
          new ResettableGenericInputStream(new S3StreamCreator(conn, bucketName, nextPath), tracker,
                  ResettableGenericInputStream.DEFAULT_BUF_SIZE, inputCharset,
              decodeErrorPolicy);
      EventDeserializer deserializer = EventDeserializerFactory.getInstance
          (deserializerType, deserializerContext, in);

      return Optional.of(new FileInfo(file, deserializer));
    } catch (FileNotFoundException e) {
      // File could have been deleted in the interim
      logger.warn("Could not find file: " + file, e);
      return Optional.absent();
    } catch (IOException e) {
      logger.error("Exception opening file: " + file, e);
      return Optional.absent();
    }
  }

  private void deleteMetaFile() throws IOException {
    if (metaFile.exists() && !metaFile.delete()) {
      throw new IOException("Unable to delete old meta file " + metaFile);
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
    private File backingDirectory;
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

    public Builder setS3Client(AmazonS3Client s3Client) {
      this.s3Client = s3Client;
      return this;
    }

    public Builder backingDirectory(File directory) {
      this.backingDirectory = directory;
      return this;
    }

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

    
    public S3ObjectEventReader build() throws IOException {
      return new S3ObjectEventReader(backingDirectory, bucketName, deserializerType,
                 deserializerContext, inputCharset,  decodeErrorPolicy, s3Client);
    }
  }

}
