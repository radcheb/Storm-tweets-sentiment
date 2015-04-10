package com.radcheb.stormtweets;

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

public class StemmingBolt extends BaseRichBolt {

	private static final long serialVersionUID = 42L;
	private static Logger LOGGER = Logger.getLogger(StemmingBolt.class);
	private OutputCollector collector = null;

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		LOGGER.debug("removing stop words");
		long id = input.getLongByField(Params.TWEET_ID_FIELD);
		String text = input.getStringByField(Params.TWEET_TEXT_FIELD);
		List<String> stopWords = StopWords.getWords();
		for(String word :stopWords){
			text =  text.replace("\\b"+word+"\\b", "");
		}
		collector.emit(new Values(id,text));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(Params.TWEET_ID_FIELD,
				Params.TWEET_TEXT_FIELD));
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
