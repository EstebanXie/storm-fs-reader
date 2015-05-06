package fs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

public class LocalReader extends FSReader {
  private static final Logger LOG = LoggerFactory.getLogger("LocalReader");

  /**
   * Open the DataInputStream associated with this FS
   *
   * @throws IOException
   */
  @Override
  public InputStream getStream(String path) throws IOException {
    return new BufferedInputStream(new FileInputStream(path));
  }

  /**
   * Get the list of files for this file system reader (Note that it points at a path)
   *
   * @return
   */
  @Override
  public String[] getFileList(String path) throws IOException {
    File folder = new File(path);
    if (folder.isDirectory())
      return folder.list();
    else {
      // Return a list only containing the file
      String stump[] = {folder.getAbsolutePath()};
      return stump;
    }
  }
}
