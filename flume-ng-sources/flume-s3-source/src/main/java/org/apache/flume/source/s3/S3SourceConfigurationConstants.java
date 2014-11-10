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

import org.apache.flume.serialization.DecodeErrorPolicy;

public class S3SourceConfigurationConstants {
  public static final String ACCESS_KEY = "accessKey";
  public static final String SECRET_KEY = "secretKey";
  public static final String BUCKET_NAME = "bucketName";
  public static final String END_POINT = "endPoint";
  public static final String BACKING_STORE = "backingStore";

  public static final String BATCH_SIZE = "batchSize";
  public static final String BACKING_DIR = "backingDir";

  public static final int DEFAULT_BATCH_SIZE = 100;
  public static final String DEFAULT_BACKING_STORE = "file";

  /** Deserializer to use to parse the file data into Flume Events */
  public static final String DESERIALIZER = "deserializer";
  public static final String DEFAULT_DESERIALIZER = "LINE";

  /** Character set used when reading the input. */
  public static final String INPUT_CHARSET = "inputCharset";
  public static final String DEFAULT_INPUT_CHARSET = "UTF-8";

  /** What to do when there is a character set decoding error. */
  public static final String DECODE_ERROR_POLICY = "decodeErrorPolicy";
  public static final String DEFAULT_DECODE_ERROR_POLICY = DecodeErrorPolicy.FAIL.name();

  public static final String MAX_BACKOFF = "maxBackoff";

  public static final Integer DEFAULT_MAX_BACKOFF = 4000;
}
