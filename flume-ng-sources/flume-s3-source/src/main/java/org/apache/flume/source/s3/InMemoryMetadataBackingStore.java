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

import org.apache.flume.serialization.PositionTracker;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class InMemoryMetadataBackingStore extends MetadataBackingStore {

  Set set = new HashSet<String>();
  public InMemoryMetadataBackingStore(String name) {
    super(name);
  }

  @Override
  void init() {

  }

  @Override
  void remove(String key) {
    set.remove(key);
  }

  @Override
  void add(String key) {
    set.add(key);
  }

  @Override
  boolean contains(String key) {
    return set.contains(key);
  }

  @Override
  void close() throws IOException {}

  @Override
  PositionTracker getPositionTracker(String key) {
    return new TransientPositionTracker(key);
  }

  @Override
  void resetPositionTracker() {
    // no-op
  }

  static class TransientPositionTracker implements PositionTracker {

    private final String target;
    private long position = 0;

    public TransientPositionTracker(String target) {
      this.target = target;
    }

    @Override
    public void storePosition(long position) throws IOException {
      this.position = position;
    }

    @Override
    public long getPosition() {
      return position;
    }

    @Override
    public String getTarget() {
      return target;
    }

    @Override
    public void close() throws IOException {
      // no-op
    }
  }
}
