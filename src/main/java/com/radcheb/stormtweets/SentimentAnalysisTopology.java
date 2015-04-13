package com.radcheb.stormtweets;

import java.util.Arrays;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class SentimentAnalysisTopology {

	private final Logger LOGGER = Logger.getLogger(this.getClass());
	private static final String KAFKA_TOPIC = Properties
			.getString("rts.storm.kafka_topic");

	private static boolean remote = true;

	public static void main(String[] args) throws Exception {
		BasicConfigurator.configure();

		if (remote) {
			System.setProperty("storm.jar", "/home/sniperking/workspace/Storm-tweets-sentiment/target/storm-tweets-sentiment-1.0-SNAPSHOT.jar");
			StormSubmitter submitter = new StormSubmitter();
			submitter.submitTopology("sentiment-analysis", createConfig(false), createTopology());

		} else {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("sentiment-analysis", createConfig(true),
					createTopology());
			Thread.sleep(6000000);
			cluster.shutdown();
		}
	}

	private static StormTopology createTopology() {
		SpoutConfig kafkaConf = new SpoutConfig(new ZkHosts(
				Properties.getString("rts.storm.zkhosts")), KAFKA_TOPIC,
				"/kafka", "KafkaSpout");
		kafkaConf.scheme = new SchemeAsMultiScheme(new StringScheme());
		TopologyBuilder topology = new TopologyBuilder();

		topology.setSpout("kafka_spout", new KafkaSpout(kafkaConf), 4);

		topology.setBolt("twitter_filter", new TwitterFilterBolt(), 4)
				.shuffleGrouping("kafka_spout");

		topology.setBolt("text_filter", new TextFilterBolt(), 4)
				.shuffleGrouping("twitter_filter");

		topology.setBolt("stemming", new StemmingBolt(), 4).shuffleGrouping(
				"text_filter");

		topology.setBolt("positive", new PositiveSentimentBolt(), 4)
				.shuffleGrouping("stemming");
		topology.setBolt("negative", new NegativeSentimentBolt(), 4)
				.shuffleGrouping("stemming");

		topology.setBolt("join", new JoinSentimentsBolt(), 4)
				.fieldsGrouping("positive", new Fields("tweet_id"))
				.fieldsGrouping("negative", new Fields("tweet_id"));

		topology.setBolt("score", new SentimentScoringBolt(), 4)
				.shuffleGrouping("join");

//		topology.setBolt("hdfs", new HDFSBolt(), 4).shuffleGrouping("score");
		topology.setBolt("nodejs", new NodeNotifierBolt(), 4).shuffleGrouping(
				"score");

		return topology.createTopology();
	}

	private static Config createConfig(boolean local) {
		int workers = Properties.getInt("rts.storm.workers");
		Config conf = new Config();
		conf.setDebug(true);
		if(remote){
			conf.put(Config.NIMBUS_HOST, "172.17.42.1");
			conf.put(Config.NIMBUS_THRIFT_PORT, 49627);
			conf.put(Config.STORM_ZOOKEEPER_SERVERS,  Arrays.asList(new String[]{"localhost"}));
			conf.put(Config.STORM_ZOOKEEPER_PORT, 49181);
			conf.setNumWorkers(workers);
			conf.setMaxSpoutPending(5000);
		}
		if (local)
			conf.setMaxTaskParallelism(workers);
		else
			conf.setNumWorkers(workers);
		return conf;
	}

}
