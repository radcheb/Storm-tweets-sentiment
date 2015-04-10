package com.radcheb.stormtweets;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

public class StopWords implements Serializable{

    public static final long serialVersionUID = 42L;
    private List<String> stopWords;
    private static StopWords _singleton;
    
    private StopWords(){
    	this.stopWords = new ArrayList<String>();
    	BufferedReader reader = null;
    	try{
    		reader = new BufferedReader(
    				new InputStreamReader(
    						this.getClass().getResourceAsStream(Params.STOP_WORDS_SRC)));
    		String line = null;
    		while( (line = reader.readLine()) != null){
    			this.stopWords.add(line);
    		}
    	}catch(IOException e){
    		Logger.getLogger(this.getClass()).error("IO error while initializing stop words",e);
    	}finally{
    		try{
    			if(reader != null){
    				reader.close();
    			}
    		}catch (IOException e) {
    	    		Logger.getLogger(this.getClass()).error("IO error while stop words reader",e);
    			}
    		}
    	}
    
    private static StopWords get(){
    	if(_singleton == null){
    		_singleton = new StopWords();
    	}
		return _singleton;
    }
    public static List<String> getWords(){
    	return get().stopWords;
    }
}

