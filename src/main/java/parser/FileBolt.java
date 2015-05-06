package parser;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import fs.FSFactory;
import fs.FSReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;

// For now assume that each file does not change but new files may show up in the parent directory
public class FileBolt extends BaseBasicBolt {
  private static Logger LOG = LoggerFactory.getLogger("FileBolt");
  boolean completed = false;
  
  // Define a fsReader interface to eliminate Hadoop dependency. At creation we define which type
  // of fsReader to use
  private FSReader fsReader = null;

  @Override
  public void prepare(Map stormConf, TopologyContext context) {
    fsReader = FSFactory.get(stormConf.get("fs.reader"));
  }

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    String path = tuple.getString(0);
    
    try {
      BufferedReader reader = new BufferedReader(new InputStreamReader(fsReader.getStream(path)));
      String line = reader.readLine();
      
      while (line != null) {
        collector.emit(new Values(line));
        line = reader.readLine();
      }
      
      reader.close();
    } catch (IOException e) {
      LOG.error("Error when opening/reading file: " + path, e);
    }
    finally {
      completed = true;
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("fileLine"));
  }
}
