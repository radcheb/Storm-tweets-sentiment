package com.radcheb.stormtweets;

import java.io.IOException;
import java.util.Map;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.log4j.Logger;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class NodeNotifierBolt extends BaseRichBolt 
{

    private static final long serialVersionUID = 42L;
    private static final Logger LOGGER =
        Logger.getLogger(NodeNotifierBolt.class);
    private String webserver = Properties.getString("rts.storm.webserv");
    private HttpClient client;

    @Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
    	reconnect();
	}

    private void reconnect(){
    	this.client = HttpClientBuilder.create().build();
    }
    @Override
	public void execute(Tuple input) {
        Long id = input.getLong(input.fieldIndex("tweet_id"));
        String tweet = input.getString(input.fieldIndex("tweet_text"));
        int pos = input.getInteger(input.fieldIndex("pos_score"));
        int neg = input.getInteger(input.fieldIndex("neg_score"));
        String score = input.getString(input.fieldIndex("score"));
        HttpPost post = new HttpPost(this.webserver);
        String content = 
        		String.format(

        	            "{\"id\": \"%d\", "  +
        	            "\"tweet\": \"%s\", " +
        	            "\"pos\": \"%d\", "  +
        	            "\"neg\": \"%d\", "  +
        	            "\"score\": \"%s\" }",
        	         	id,tweet,pos,neg,score);
        try{
        	post.setEntity(new StringEntity(content));
        	HttpResponse response = client.execute(post);
        	org.apache.http.util.EntityUtils.consume(response.getEntity());
        }catch(IOException e){
            LOGGER.error("exception thrown while attempting post", e);
            LOGGER.trace(null, e);
            reconnect();
        }catch (NullPointerException e) {
            LOGGER.error("exception thrown while attempting post", e);
            LOGGER.debug("server: "+this.webserver);
            LOGGER.debug("client: "+client.toString());
            LOGGER.debug("server: "+post.toString());
            LOGGER.trace(null, e);
		}
    }
	/*
	 * Terminal Bolt, nothing to declare
	 * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {		
	}

}
