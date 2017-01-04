package Storm_Kafka.storm_kafka;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * Hello world!
 *
 */
public class MyKafkaLogTopology 
{
	
	public static class KafkaTrackingExtractor extends BaseRichBolt {

		private static final long serialVersionUID = 1L;
		private OutputCollector collector;
		
//		@Override
		public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
			// TODO Auto-generated method stub
			this.collector = collector;
		}
		
//		@Override
		public void execute(Tuple input) {
			// TODO Auto-generated method stub
			String line = input.getString(0);
			System.out.println("Receive[Kafka -> extractor]" + line);
			
			if (line.contains("/tracking/click?")) {
				collector.emit(input, new Values("total_click", 1));
				collector.ack(input);
				
				if (line.contains("action=Kids")) {
					collector.emit(input, new Values("Kids_click", 1));
				} else if (line.contains("action=Men")) {
					collector.emit(input, new Values("Mens_click", 1));
				} else if (line.contains("action=Women")) {
					collector.emit(input, new Values("Womens_click", 1));
				}
				
			} else {
				collector.emit(input, new Values("total_click", 0));
			}
				
						
		}

		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			// TODO Auto-generated method stub
			declarer.declare(new Fields("click", "count"));
		}
	}
	
	public static class ClickCounter extends BaseRichBolt {

		private static final long serialVersionUID = 1L;
		private OutputCollector collector;
		private Map<String, AtomicInteger> counterMap;

		public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
			// TODO Auto-generated method stub
			this.collector = collector;
			this.counterMap = new HashMap<String, AtomicInteger>();
		}
		
		public void execute(Tuple input) {
			// TODO Auto-generated method stub
			String word = input.getString(0);
			int count = input.getInteger(1);
			System.out.println("Receive[extractor -> counter]" + word + " : " + count);
			AtomicInteger ai = this.counterMap.get(word);
			if (ai == null) {
				ai = new AtomicInteger();
				this.counterMap.put(word, ai);
			}
			ai.addAndGet(count);
			collector.ack(input);
			System.out.println("Check statistics map: " + this.counterMap);
			
			AtomicInteger aiTotal = counterMap.get("total_click");
			AtomicInteger aiKids = counterMap.get("Kids_click");
			AtomicInteger aiMens = counterMap.get("Mens_click");
			AtomicInteger aiWomens = counterMap.get("Womens_click");
			int total_click = (aiTotal == null) ? 0 : aiTotal.intValue();
			int kids_click = (aiKids == null) ? 0 : aiKids.intValue();
			int mens_click = (aiMens == null) ? 0 : aiMens.intValue();
			int womens_click = (aiWomens == null) ? 0 : aiWomens.intValue();
			if (total_click != 0) {
				System.out.println(String.format("Kids click ratio is %.3f", ((double) kids_click) / total_click));
				System.out.println(String.format("Mens click ratio is %.3f", ((double) mens_click) / total_click));
				System.out.println(String.format("Womens click ratio is %.3f", ((double) womens_click) / total_click));
			}
		}
		
		public void cleanup() {
			System.out.println("The final results:");
			Iterator<Entry<String, AtomicInteger>> iter = this.counterMap.entrySet().iterator();
			while (iter.hasNext()) {
				Entry<String, AtomicInteger> entry = iter.next();
				System.out.println(entry.getKey() + "\t:\t" + entry.getValue().get());
			}
		}

		
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			// TODO Auto-generated method stub
			declarer.declare(new Fields("click", "count"));
		}
		
	}
	
	
    public static void main( String[] args ) throws Exception
    {
    	String zks = "iMac2015:2181,MacBookPro2012:2181,MacBookPro2013:2181";
    	String topic = "tracking_accesslog_topic";
    	String zkRoot = "/storm";
    	String id = "click";
    	
    	BrokerHosts brokerHosts = new ZkHosts(zks);
    	SpoutConfig spoutConf = new SpoutConfig(brokerHosts, topic, zkRoot, id);
    	spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
    	spoutConf.zkServers = Arrays.asList(new String[] {"iMac2015", "MacBookPro2012", "MacBookPro2013"});
    	spoutConf.zkPort = 2181;
    	
    	TopologyBuilder builder = new TopologyBuilder();
    	builder.setSpout("Kafka-reader", new KafkaSpout(spoutConf), 4);
    	builder.setBolt("click-extractor", new KafkaTrackingExtractor(), 2).shuffleGrouping("Kafka-reader");
    	builder.setBolt("click-counter", new ClickCounter()).fieldsGrouping("click-extractor", new Fields("click"));
    	
    	Config conf = new Config();
    	
    	String name = MyKafkaLogTopology.class.getSimpleName();
    	
    	
    	conf.put(Config.NIMBUS_HOST, "iMac2015");
    	conf.setNumWorkers(3);
    	StormSubmitter.submitTopologyWithProgressBar(name, conf, builder.createTopology());
    	
    }
    
}
