package com.radcheb.stormtweets;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;

public class NegativeWords implements Serializable {

	public static final long serialVersionUID = 42L;
	private Set<String> negWords;
	private static NegativeWords _singleton;

	private NegativeWords() {
		negWords = new HashSet<String>();
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new InputStreamReader(this.getClass()
					.getResourceAsStream(Params.NEGATIVE_WORDS_SRC)));
			String line;
			while ((line = reader.readLine()) != null) {
				this.negWords.add(line);
			}
		} catch (IOException e) {
			Logger.getLogger(this.getClass()).error(
					"IO error while initializing negative words", e);
		}finally{
			try{
				if(reader != null){
					reader.close();
				}
			}catch(IOException e){
				Logger.getLogger(this.getClass()).error(
						"IO error while closing negative words reader", e);
			}
		}
	}

	private static NegativeWords get(){
		if(_singleton == null){
			_singleton = new NegativeWords();
		}
		return _singleton;
	}

	public static Set<String> getWords(){
		return get().negWords;
	}
}
