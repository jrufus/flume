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

import com.google.common.base.Preconditions;
import org.mapdb.DBMaker;
import org.mapdb.DB;

import java.io.File;
import java.io.IOException;
import java.util.Set;

/*
 * Uses MapDB, to store the list of files already processed
 */
public class FileBasedMetadataBackingStore extends MetadataBackingStore {
  private DB db;
  private Set<String> set;
  public FileBasedMetadataBackingStore(String name, File backingDir) {
    super(name);
    // Verify directory exists and is readable/writable
    Preconditions.checkState(backingDir.exists(),
            "Directory does not exist: " + backingDir.getAbsolutePath());
    Preconditions.checkState(backingDir.isDirectory(),
            "Path is not a directory: " + backingDir.getAbsolutePath());

    File dbFile = new File(backingDir, name + ".db");
    db = DBMaker.newFileDB(dbFile)
            .closeOnJvmShutdown()
            .mmapFileEnableIfSupported()
            .make();
    set = db.getHashSet("MapDBSet " + " - " + name);
  }

  void remove(String key) {
    set.remove(key);
    db.commit();
  }

  void add(String key) {
    set.add(key);
    db.commit();
  }

  boolean contains(String key) {
    return set.contains(key);
  }

  void close() throws IOException {
    db.close();
  }
}
