package org.apache.flume.source.s3;

import com.google.common.collect.ImmutableSortedSet;

import java.io.IOException;

/**
 * Created by jrufus on 10/17/14.
 */

abstract class MetadataBackingStore {
  private final String name;

  protected MetadataBackingStore(String name) {
    this.name = name;
  }

  abstract void remove(String key);
  abstract void add(String key);
  abstract boolean contains(String key);
  abstract void close() throws IOException;

  String getName() {
    return name;
  }

}
