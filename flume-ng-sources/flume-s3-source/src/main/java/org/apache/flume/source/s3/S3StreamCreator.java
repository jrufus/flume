package org.apache.flume.source.s3;

import java.io.InputStream;
import com.amazonaws.services.s3.AmazonS3;
import org.apache.flume.serialization.StreamCreator;

/**
 * Created by jrufus on 10/23/14.
 */
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
