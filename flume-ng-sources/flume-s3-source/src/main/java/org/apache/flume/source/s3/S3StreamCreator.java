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

import java.io.InputStream;
import com.amazonaws.services.s3.AmazonS3;
import org.apache.flume.serialization.StreamCreator;

public class S3StreamCreator implements StreamCreator {
  private AmazonS3 conn;
  private String bucketName;
  private String key;


  public S3StreamCreator(AmazonS3 conn, String bucketName,  String key) {
    this.conn = conn;
    this.bucketName = bucketName;
    this.key = key;
  }

  @Override
  public InputStream create() {
    return conn.getObject(bucketName, key).getObjectContent();
  }
}
