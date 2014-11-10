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
import java.util.HashSet;
import java.util.Set;

public class FileBasedMetadataBackingStore extends MetadataBackingStore {

  Set set = new HashSet<String>();
  public FileBasedMetadataBackingStore(String name, File backingDir) {
    super(name);
    // Verify directory exists and is readable/writable
    Preconditions.checkState(backingDir.exists(),
            "Directory does not exist: " + backingDir.getAbsolutePath());
    Preconditions.checkState(backingDir.isDirectory(),
            "Path is not a directory: " + backingDir.getAbsolutePath());

    File dbFile = new File(backingDir, name + ".db");
    DB db = DBMaker.newFileDB(dbFile)
            .closeOnJvmShutdown()
            //.cacheSize() TODO: Investigate this option
            .mmapFileEnableIfSupported()
            .make();
    set = db.createHashSet("MapDBSet " + " - " + name).make();
  }

  void remove(String key) {
    set.remove(key);
  }

  void add(String key) {
    set.add(key);
  }

  boolean contains(String key) {
    return set.contains(key);
  }

  void close() throws IOException {}
  
}
