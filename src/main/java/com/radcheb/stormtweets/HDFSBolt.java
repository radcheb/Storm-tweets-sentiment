package com.radcheb.stormtweets;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class HDFSBolt extends BaseRichBolt {

	private static final long serialVersionUID = 42L;
	private static final Logger LOGGER = Logger.getLogger(HDFSBolt.class);
	private OutputCollector collector;
	private int id;
	private List<String> tweet_scores;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		this.id = context.getThisTaskId();
		this.tweet_scores = new ArrayList<>(1000);
	}

	@Override
	public void execute(Tuple input) {

		long id = input.getLongByField(Params.TWEET_ID_FIELD);
		String tweet = input.getStringByField(Params.TWEET_TEXT_FIELD);
		int pos = input.getIntegerByField(Params.POSITIVE_SCORE_FIELD);
		int neg = input.getIntegerByField(Params.NEGATIVE_SCORE_FIELD);
		String score = input.getStringByField(Params.SCORE_FIELD);
		String tweet_score = String.format("%s, %s, %d, %d, %s\n", id, tweet,
				pos, neg, score);
		if (this.tweet_scores.size() >= 10) {
			writeToHDFS();
			this.tweet_scores = new ArrayList<String>(1000);
		}
	}

	private void writeToHDFS() {
		FileSystem hdfs = null;
		Path file = null;
		OutputStream os = null;
		BufferedWriter writer = null;
		try {
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS","172.17.42.1:9000");
			conf.set("dfs.replication","1");
//			conf.addResource(new Path("/opt/hadoop/etc/hadoop/core-site.xml"));
//			conf.addResource(new Path("/opt/hadoop/etc/hadoop/hdfs-site.xml"));
			hdfs = FileSystem.get(conf);
			file = new Path(Properties.getString("rts.storm.hdfs_output_file")
					+ this.id);
			if (hdfs.exists(file)) {
				os = hdfs.append(file);
			} else {
				os = hdfs.create(file);
			}
			writer = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
			for (String tweet_score : tweet_scores) {
				writer.write(tweet_score);
			}
		} catch (IOException e) {
			LOGGER.error("Failed to write tweet score to HDFS", e);
			LOGGER.trace(null, e);
		} finally {
			try {
				if (os != null)
					os.close();
				if (writer != null)
					writer.close();
				if (hdfs != null)
					hdfs.close();
			} catch (IOException e) {
				LOGGER.fatal("IO Exception thrown while closing HDFS", e);
				LOGGER.trace(null, e);
			}
		}
	}

	/*
	 * Terminal bolt, nothing to declare
	 * 
	 * @see
	 * backtype.storm.topology.IComponent#declareOutputFields(backtype.storm
	 * .topology.OutputFieldsDeclarer)
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}
