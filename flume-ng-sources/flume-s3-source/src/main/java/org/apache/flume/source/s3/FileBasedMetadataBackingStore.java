/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flume.source.s3;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.io.Files;
import org.apache.flume.Context;
import org.apache.flume.FlumeException;
import org.apache.flume.serialization.DurablePositionTracker;
import org.apache.flume.serialization.PositionTracker;
import org.mapdb.DBMaker;
import org.mapdb.DB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.apache.flume.source.s3.S3SourceConfigurationConstants.BACKING_DIR;

/*
 * Uses MapDB, to store the list of files already processed
 */
public class FileBasedMetadataBackingStore extends MetadataBackingStore {
  private DB db;
  private Set<String> set;
  private String name;
  private File backingDir;
  private File metaFile;

  static final String metaFileName = ".flumes3-main.meta";

  private static final Logger logger = LoggerFactory
          .getLogger(S3ObjectEventReader.class);

  public FileBasedMetadataBackingStore(String name, Context context) {
    super(name);
    this.name = name;
    String backingDirPath = context.getString(BACKING_DIR);
    Preconditions.checkState(backingDirPath != null, "Configuration must specify an existing directory for storing metadata");

    this.backingDir = new File(backingDirPath);
    // Verify directory exists and is readable/writable
    Preconditions.checkState(backingDir.exists(), "Configuration must specify an existing backing directory");
    Preconditions.checkState(backingDir.isDirectory(),
            "Path is not a directory: " + backingDir.getAbsolutePath());
    // Do a canary test to make sure we have access to backing directory
    try {
      File canary = File.createTempFile("flume-s3dir-perm-check-", ".canary",
              backingDir);
      Files.write("testing flume file permissions\n", canary, Charsets.UTF_8);
      List<String> lines = Files.readLines(canary, Charsets.UTF_8);
      Preconditions.checkState(!lines.isEmpty(), "Empty canary file %s", canary);
      if (!canary.delete()) {
        throw new IOException("Unable to delete canary file " + canary);
      }
      logger.debug("Successfully created and deleted canary file: {}", canary);
    } catch (IOException e) {
      throw new FlumeException("Unable to read and modify files" +
              " in the backing directory: " + backingDir, e);
    }

    File trackerDirectory = new File(backingDir, "tracker");

    // ensure that meta directory exists
    if (!trackerDirectory.exists()) {
      if (!trackerDirectory.mkdir()) {
        throw new FlumeException("Unable to mkdir nonexistent meta directory " +
                trackerDirectory);
      }
    }

    // ensure that the meta directory is a directory
    if (!trackerDirectory.isDirectory()) {
      throw new FlumeException("Specified meta directory is not a directory" +
              trackerDirectory);
    }

    this.metaFile = new File(trackerDirectory, metaFileName);
  }

  @Override //TODO: CAll this everywhere in the factory
  void init() {
    File dbFile = new File(backingDir, name + ".db");
    db = DBMaker.newFileDB(dbFile)
            .closeOnJvmShutdown()
            .mmapFileEnableIfSupported()
            .make();
    set = db.getHashSet("MapDBSet " + " - " + name);
  }

  @Override
  void remove(String key) {
    set.remove(key);
    db.commit();
  }

  @Override
  void add(String key) {
    set.add(key);
    db.commit();
  }

  @Override
  boolean contains(String key) {
    return set.contains(key);
  }

  @Override
  void close() throws IOException {
    db.close();
  }

  @Override
  PositionTracker getPositionTracker(String key) {
    PositionTracker tracker = null;
    try {
      tracker = DurablePositionTracker.getInstance(metaFile, key);
    } catch (IOException e) {
      throw new FlumeException("Error in getting position tracker " + e);
    }
    return tracker;
  }

  @Override
  void resetPositionTracker() {
    if (metaFile.exists() && !metaFile.delete()) {
      throw new FlumeException("Unable to delete old meta file " + metaFile);
    }
  }

  public static class Builder implements MetadataBackingStore.Builder {

    @Override
    public MetadataBackingStore build(String bucketName, Context context) {
      FileBasedMetadataBackingStore store = new FileBasedMetadataBackingStore(bucketName, context);
      return store;
    }

  }
}
