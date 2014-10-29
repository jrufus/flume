package org.apache.flume.source.s3;

import com.google.common.base.Preconditions;
import org.mapdb.DBMaker;
import org.mapdb.DB;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by jrufus on 10/18/14.
 */
public class FileBasedMetadataBackingStore extends MetadataBackingStore {

  Set set = new HashSet<String>();
  public FileBasedMetadataBackingStore(String name, File backingDir) throws IOException {
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
