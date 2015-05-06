package fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

/**
 * Class to allow transparent reading from both local and Hadoop file systems. This is 
 * basically a wrapper on top of hadoop.fs.FileSystem
 */
public class HadoopReader extends FSReader {
  private static Logger LOG = LoggerFactory.getLogger("HadoopReader");
  private FileSystem fs = null;
  
  public void createFs(String path) throws IOException {
    createFs(new Path(path));
  }

  public void createFs(Path path) throws IOException {
    Configuration conf = new Configuration();
    conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
    conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));

    try {
      // Use getFileSystem to get the correct file system type for this path. 
      fs = path.getFileSystem(conf);
    } catch (IOException e) {
      LOG.error("Failed to read hadoop path: \n" + toAbsPath(path), e);
      throw new IOException(e);
    }
  }

  private static String toAbsPath(Path path) {
    String pathString = path.toString();
    boolean isFile = pathString.startsWith("file:");
    boolean isHadoop = pathString.startsWith("hdfs:");
    if (isFile || isHadoop) {
      return pathString.substring(5, pathString.length()-1);
    }
    if (!pathString.startsWith("/")) throw new IllegalArgumentException("Invalid path, ensure path" +
      "starts with either `file://`, `hdfs://`, or `/`. Path: " + pathString);
    return pathString;
  }
  
  public FSDataInputStream open(Path path) throws IOException {
    if (fs == null)
      createFs(path);
    
    return fs.open(path);
  }

  @Override
  public InputStream getStream(String path) throws IOException {
    return open(new Path(path));
  }
  
  @Override
  public String[] getFileList(String path) throws IOException {
    if (fs == null)
      createFs(path);
    
    FileStatus status[] = fs.listStatus(new Path(path));
    String stringArr[] = new String[status.length];
    for (int i = 0; i < status.length; i++) {
      stringArr[i] = toAbsPath(status[i].getPath());
    }
    
    return stringArr;
  }
}
