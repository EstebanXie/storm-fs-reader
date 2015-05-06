package fs;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.IOException;
import java.io.InputStream;

/**
 * Create an abstraction for interacting with the underlying file system. We want to do this
 * since we want to be able to interact with different file systems, e.g. Hadoop vs. a local 
 * UNIX file system but not expose all of the underlying libraries (we may not have Hadoop 
 * installed).
 */
public abstract class FSReader {
  public enum FS {
    LOCAL,
    HADOOP
  }
  
  /**
   * Open the DataInputStream associated with this FS
   * 
   * Note: The caller must close this stream once complete
   * @throws IOException
   */
  public InputStream getStream(String path) throws IOException {
    throw new NotImplementedException();
  }

  /**
   * Get the list of files for this file system reader (Note that it points at a path)
   * @return
   */
  public String[] getFileList(String path) throws IOException {
    throw new NotImplementedException();
  }
}

