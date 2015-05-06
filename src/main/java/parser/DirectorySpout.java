package parser;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import fs.FSFactory;
import fs.FSReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Create a spout that generates a list of files to process in a directory on either HDFS or 
 * local FS. If new files are added they will be processed on the next iteration.
 */
public class DirectorySpout extends BaseRichSpout {
  private String fileDir;
  private FSReader fsReader = null;
  private static Logger LOG = LoggerFactory.getLogger("DirectorySpout");
  
  private TopologyContext context = null;
  private SpoutOutputCollector collector = null;
  
  private Set<String> workingSet = new HashSet<String>();

  // every 30 minutes, check for new files to process
  private final static long WAITING_TIME_MS = TimeUnit.SECONDS.toMillis(2);

  public DirectorySpout(String fileDir) {
    this.fileDir = fileDir;
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("files"));
  }

  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    this.context = context;
    this.collector = collector;
    this.fsReader = FSFactory.get(conf.get("fs.reader"));
  }

  @Override
  public void nextTuple() {
    try {
      LOG.info("Getting file list.");
      String[] files = fsReader.getFileList(fileDir);
      
      if (files != null) {
        for (String path: files) {
          // Maintain a working set to support new files showing up in the directory
          if (!workingSet.contains(path)) {
            this.collector.emit(new Values(path), path);
            
            //TODO Uncomment this, for now run on loop to test things out.
            //workingSet.add(path);  
          }
        }
      }
      
      Thread.sleep(WAITING_TIME_MS);
    } catch(InterruptedException e) {
      LOG.error("Thread sleep interrupted in nextTuple().");
    } catch(IOException e) {
      LOG.error("Failed to list files for provided path " + fileDir);
    } catch(Exception e) {
      LOG.error("Error reading tuple", e);
    }
  }

  @Override
  public void ack(Object filePath) {
    //  msgId should be "ID 6"
    if (filePath == null)
    {
      LOG.error("Ack was null in DirectorySpout.");
    }
    else {
      LOG.info("[DirectorySpout]" + "ack for file: " + filePath.toString());  
    }
    
  }

  @Override
  public void fail(Object filePath) {
    // remove the file from the working set
    workingSet.remove((String) filePath);
    LOG.error("Failed to process: " + filePath.toString());

  }
}
