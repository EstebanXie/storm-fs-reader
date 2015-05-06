import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

import fs.FSReader;
import parser.DirectorySpout;
import parser.FileBolt;
import parser.SplitBolt;
import worker.MapNullCounterBolt;

/**
 * Class to wrap topology creation and execution
 */
public class Parser {
  private TopologyBuilder builder;
  private Config conf;
  
  public Parser(String fileDir, String schemaPath) {
    this.builder = new TopologyBuilder();
    this.conf = new Config();
    conf.setDebug(true);
    conf.put("fs.reader", FSReader.FS.LOCAL.toString());

    builder.setSpout("files", new DirectorySpout(fileDir), 1);
    builder.setBolt("fileLine", new FileBolt(), 4).shuffleGrouping("files");
    builder.setBolt("fieldMap", new SplitBolt(",", ",", schemaPath), 10)
      .shuffleGrouping("fileLine");
    builder.setBolt("nullFrac", new worker.MapNullCounterBolt(), 10).shuffleGrouping("fieldMap");
  }
  
  public void run() {
    conf.setNumWorkers(3);

    try {
      StormSubmitter.submitTopologyWithProgressBar("Parser", conf, builder.createTopology());
    } catch (AlreadyAliveException e) {
      e.printStackTrace();
    } catch (InvalidTopologyException e) {
      e.printStackTrace();
    }
  }
  
  public void test() {
    LocalCluster cluster = new LocalCluster();
    cluster.submitTopology("test", conf, builder.createTopology());
    Utils.sleep(200000);
    cluster.killTopology("test");
    cluster.shutdown();
  }
}
