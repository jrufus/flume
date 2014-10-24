package org.apache.flume.serialization;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

/**
 * Created by jrufus on 10/23/14.
 */
public class FileStreamCreator implements StreamCreator {
  private File file;

  public FileStreamCreator(File file) {
    this.file = file;
  }

  @Override
  public InputStream create() {
    FileInputStream fis;
    try {
      fis = new FileInputStream(file);
    } catch(FileNotFoundException ex) {
      ex.printStackTrace();
      throw new RuntimeException(ex);
    }
    return fis;
  }
}
