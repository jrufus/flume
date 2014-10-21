package org.apache.flume.source.s3;

import org.apache.flume.serialization.DecodeErrorPolicy;

/**
 * Created by jrufus on 10/17/14.
 */
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
