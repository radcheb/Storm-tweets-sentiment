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

public class NegativeSentimentBolt extends BaseRichBolt {

	private static final long serialVersionUID = 42L;
	private static final Logger LOGGER = Logger
			.getLogger(NegativeSentimentBolt.class);
	private OutputCollector collector = null;

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector= collector;
	}

	@Override
	public void execute(Tuple input) {
		long id = input.getLongByField(Params.TWEET_ID_FIELD);
		String text = input.getStringByField(Params.TWEET_TEXT_FIELD);
		Set<String> negWords = NegativeWords.getWords();
		String[] words = text.split(" ");
		int numNegWords = 0;
		for(String word : words){
			if(negWords.contains(word)){
				numNegWords++;
			}
		}
		collector.emit(new Values(id,numNegWords,text));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(Params.TWEET_ID_FIELD,Params.NEGATIVE_SCORE_FIELD,Params.TWEET_TEXT_FIELD));
	}

}
