package com.radcheb.stormtweets;

import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class PositiveSentimentBolt extends BaseRichBolt {

	private static final long serialVersionUID = 42L;
	private static final Logger LOGGER = Logger
			.getLogger(PositiveSentimentBolt.class);
	private OutputCollector collector = null;

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector=collector;
	}

	@Override
	public void execute(Tuple input) {
		LOGGER.debug("Calculating positive score");
		long id = input.getLongByField(Params.TWEET_ID_FIELD);
		String text = input.getStringByField(Params.TWEET_TEXT_FIELD);
		Set<String> posWords = PositiveWords.getWords();
		String[] words = text.split(" ");
		int numPosWords = 0;
		for(String word : words){
			if(posWords.contains(word)){
				numPosWords++;
			}
		}
		collector.emit(new Values(id,numPosWords,text));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(Params.TWEET_ID_FIELD,Params.POSITIVE_SCORE_FIELD,Params.TWEET_TEXT_FIELD));
	}

}
