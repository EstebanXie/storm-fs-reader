package worker;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Map;

/**
 * Simple test bolt to compute the percentage of empty fields in a given map of Strings.
 */
public class MapNullCounterBolt extends BaseBasicBolt {
  private static Logger LOG = LoggerFactory.getLogger("SplitBolt");
  
  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    Map map = (Map) tuple.getValue(0);
    int nulls = 0;
    int total = 0;
    
    // TODO Is this casting ok?
    for(String string : ((Map<String, String>) map).values()) {
      if(string.length() == 0) {
        nulls++;
      }
      total++;
    }
    
    double fraction = nulls/(double) total;
    NumberFormat formatter = new DecimalFormat("#0.00");
    LOG.info(formatter.format(fraction) + "% of this map is nulls.");
    collector.emit(new Values(fraction));
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("nullFrac"));
  }
}
