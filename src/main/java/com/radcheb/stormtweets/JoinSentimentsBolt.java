package com.radcheb.stormtweets;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class JoinSentimentsBolt extends BaseRichBolt {

	private static final long serialVersionUID = 42L;
	private static final Logger LOGGER = Logger
			.getLogger(JoinSentimentsBolt.class);
	private HashMap<Long, Triple<String, Integer, String>> tweets;
	private OutputCollector collector = null;

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		this.tweets = new HashMap<Long, Triple<String, Integer, String>>();
	}

	@Override
	public void execute(Tuple input) {
		long id = input.getLongByField(Params.TWEET_ID_FIELD);
		String text = input.getStringByField(Params.TWEET_TEXT_FIELD);
		if (input.contains(Params.POSITIVE_SCORE_FIELD)) {
			int pos = input.getIntegerByField(Params.POSITIVE_SCORE_FIELD);
			if (this.tweets.containsKey(id)) {
				Triple<String, Integer, String> triple = this.tweets.get(id);
				if ("neg".equals(triple.getCar())) {
					emit(id, triple.getCaar(), pos, triple.getCdr());
				} else {
					LOGGER.warn("one sided join attempted on pos but failed");
					this.tweets.remove(id);
				}
			} else {
				this.tweets.put(id, new Triple<String, Integer, String>("pos",
						pos, text));
			}
		} else if (input.contains(Params.NEGATIVE_SCORE_FIELD)) {
			LOGGER.debug("NEG: content:: "+input.getValueByField(Params.NEGATIVE_SCORE_FIELD));
			int neg = input.getIntegerByField(Params.NEGATIVE_SCORE_FIELD);
			if (this.tweets.containsKey(id)) {
				Triple<String, Integer, String> triple = this.tweets.get(id);
				if ("pos".equals(triple.getCar())) {
					emit(id, triple.getCaar(), neg, triple.getCdr());
				} else {
					LOGGER.warn("one sided join attempted on neg but failed");
					this.tweets.remove(id);
				}
			} else {
				this.tweets.put(id, new Triple<String, Integer, String>("neg",
						neg, text));
			}
		} else {
			throw new RuntimeException("What ! no score at all !!!");
		}
	}

	private List<Integer> emit(long id, String text, int pos, int neg) {
		List<Integer> result = collector.emit(new Values(id, pos, neg, text));
		this.tweets.remove(id);
		return result;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(Params.TWEET_ID_FIELD,
				Params.POSITIVE_SCORE_FIELD, Params.NEGATIVE_SCORE_FIELD,
				Params.TWEET_TEXT_FIELD));
	}

}
