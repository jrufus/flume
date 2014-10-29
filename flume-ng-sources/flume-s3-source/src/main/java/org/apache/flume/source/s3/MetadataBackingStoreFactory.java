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

import com.google.common.io.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

class MetadataBackingStoreFactory {
  private static final Logger LOG = LoggerFactory.getLogger(MetadataBackingStoreFactory.class);
  private MetadataBackingStoreFactory() {}
  public static MetadataBackingStore get(String storeType, String name, File metadataDir){
    if(storeType.equals("Memory")) {
      return new InMemoryMetadataBackingStore(name);
    } else if(storeType.equals("File")) {
      return new FileBasedMetadataBackingStore(name, metadataDir);
    } else {
      throw new IllegalArgumentException();
    }
  }
}
