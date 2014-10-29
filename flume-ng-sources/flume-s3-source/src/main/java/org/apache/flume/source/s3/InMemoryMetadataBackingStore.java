package org.apache.flume.source.s3;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by jrufus on 10/18/14.
 */
public class InMemoryMetadataBackingStore extends MetadataBackingStore{

  Set set = new HashSet<String>();
  public InMemoryMetadataBackingStore(String name) {
    super(name);
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
