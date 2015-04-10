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

import org.apache.flume.Context;
import org.apache.flume.FlumeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MetadataBackingStoreFactory {
  private MetadataBackingStoreFactory() {}

  private static final Logger logger =
          LoggerFactory.getLogger(MetadataBackingStoreFactory.class);

  public static MetadataBackingStore get(String storeType, String bucketName, Context context) {
    MetadataBackingStoreType type;
    try {
      type = MetadataBackingStoreType.valueOf(storeType);
    } catch (IllegalArgumentException e) {
      logger.debug("Not in enum, loading builder class: {}", storeType);
      type = MetadataBackingStoreType.FILE;
    }
    Class<? extends MetadataBackingStore.Builder> builderClass =
            type.getBuilderClass();
    // build the builder
    MetadataBackingStore.Builder builder;
    try {
      builder = builderClass.newInstance();
    } catch (InstantiationException ex) {
      String errMessage = "Cannot instantiate builder: " + storeType;
      logger.error(errMessage, ex);
      throw new FlumeException(errMessage, ex);
    } catch (IllegalAccessException ex) {
      String errMessage = "Cannot instantiate builder: " + storeType;
      logger.error(errMessage, ex);
      throw new FlumeException(errMessage, ex);
    }

    MetadataBackingStore store = builder.build(bucketName, context);
    store.init();
    return store;
  }
}
