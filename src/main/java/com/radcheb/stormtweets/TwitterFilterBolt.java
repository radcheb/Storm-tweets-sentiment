package com.radcheb.stormtweets;

import java.io.IOException;
import java.util.Map;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class TwitterFilterBolt extends BaseRichBolt {

	private static final long serialVersionUID = 42L;
	private static final Logger LOGGER = Logger
			.getLogger(TwitterFilterBolt.class);
	private static final ObjectMapper mapper = new ObjectMapper();
	private OutputCollector collector = null;

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		LOGGER.debug("filtering incoming tweets");
		String json = input.getString(0);
//		LOGGER.debug("-----------tweet:##"+json+"######-----------");
		
		try {
			JsonNode root = mapper.readValue(json, JsonNode.class);
			long id;
			String text;
			if (root.get("lang") != null
					&& "en".equals(root.get("lang").textValue())) {
				if (root.get("id") != null
						&& root.get("text").textValue() != null) {
					id = root.get("id").longValue();
					text = root.get("text").textValue();
					collector.emit(new Values(id, text));
				} else {
					LOGGER.debug("tweet id and/ or text was null::"+root.get("text").textValue()+"::"+root.get("id").longValue());
					LOGGER.debug("-----------tweet:##"+json+"######-----------");
				}
			} else {
				LOGGER.debug("Ignoring non-english tweet::" + root.get("lang"));
			}
		} catch (IOException e) {
			LOGGER.error("IO error while filtering tweets", e);
			LOGGER.trace(null, e);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(Params.TWEET_ID_FIELD,
				Params.TWEET_TEXT_FIELD));
	}

	public Map<String, Object> getComponenetConfiguration() {
		return null;
	}
}
