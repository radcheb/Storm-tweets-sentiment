package com.radcheb.stormtweets;

import java.util.Map;

import org.apache.commons.lang.Validate;
import org.apache.log4j.Logger;

import com.sun.jdi.Type;
import com.sun.jdi.Value;
import com.sun.jdi.VirtualMachine;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class TextFilterBolt extends BaseRichBolt {

	private static final long serialVersionUID = 42L;
	private static Logger LOGGER = Logger.getLogger(TextFilterBolt.class);
	private OutputCollector collector = null;

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		LOGGER.debug("removing ugly characters");
		long id = input.getLongByField(Params.TWEET_ID_FIELD);
		String text = input.getStringByField(Params.TWEET_TEXT_FIELD);
		text = text.replace("[^a-zA-Z\\s]", "").trim().toLowerCase();
		collector.emit(new Values(id, text));
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
