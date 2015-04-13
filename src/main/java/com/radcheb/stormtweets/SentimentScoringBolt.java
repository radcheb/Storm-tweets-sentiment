package com.radcheb.stormtweets;

import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SentimentScoringBolt extends BaseRichBolt {

	private static final long serialVersionUID = 42L;
	private static final Logger LOGGER = Logger
			.getLogger(SentimentScoringBolt.class);
	private OutputCollector collector = null;

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		long id = input.getLongByField(Params.TWEET_ID_FIELD);
		String text = input.getStringByField(Params.TWEET_TEXT_FIELD);
		int pos = input.getIntegerByField(Params.POSITIVE_SCORE_FIELD);
		int neg = input.getIntegerByField(Params.NEGATIVE_SCORE_FIELD);
		String score = pos > neg ? Params.POSITIVE_SCORE_VALUE : Params.NEGATIVE_SCORE_VALUE;
        LOGGER.debug(String.format("tweet %s: %s", id, score));
        collector.emit(new Values(id,text, pos,neg,score));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(Params.TWEET_ID_FIELD,
				Params.TWEET_TEXT_FIELD, Params.POSITIVE_SCORE_FIELD,
				Params.NEGATIVE_SCORE_FIELD, Params.SCORE_FIELD));
	}

}
