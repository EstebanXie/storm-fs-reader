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
import java.util.HashMap;
import java.util.Map;

/**
 * Create a basic bolt to split a string by a provided de-limeter and classify it with a provided
 * schema.
 */
public class SplitBolt extends BaseBasicBolt {
  String stringDelim;
  String schemaDelim;
  String schemaPath;
  FSReader fsReader = null;
  private static Logger LOG = LoggerFactory.getLogger("SplitBolt");
  
  public SplitBolt(String stringDelim, String schemaDelim, String schemaPath) {
    this.stringDelim = stringDelim;
    this.schemaDelim = schemaDelim;
    this.schemaPath = schemaPath;
  }

  @Override
  public void prepare(Map stormConf, TopologyContext context) {
    fsReader = FSFactory.get(stormConf.get("fs.reader"));
  }

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    String splitS[] = tuple.getString(0).split(stringDelim);
    
    try {
      BufferedReader is = new BufferedReader(new InputStreamReader(fsReader.getStream(schemaPath)));
      String schema[] = is.readLine().split(schemaDelim);
      HashMap<String, String> map = new HashMap<String, String>();
      
      for (int i = 0; i < splitS.length; i++) {
        map.put(schema[i], splitS[i]);
      }
      
      collector.emit(new Values(map));
      is.close();
    } catch (IOException e) {
      LOG.error("Error when reading schema file. " + schemaPath, e);
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("fieldMap"));
  }
}
