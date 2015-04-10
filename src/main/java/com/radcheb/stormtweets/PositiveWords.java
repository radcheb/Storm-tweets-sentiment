package com.radcheb.stormtweets;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;

public class PositiveWords implements Serializable {

	public static final long serialVersionUID = 42L;
	private Set<String> posWords;
	private static PositiveWords _singleton;

	private PositiveWords() {
		this.posWords = new HashSet<String>();
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new InputStreamReader(this.getClass()
					.getResourceAsStream(Params.POSITIVE_WORDS_SRC)));
			String line;
			while ((line = reader.readLine()) != null) {
				this.posWords.add(line);
			}
		} catch (IOException e) {
			Logger.getLogger(this.getClass()).error(
					"IO error while initializing positive words	", e);
		} finally {
			try {
				if (reader != null) {
					reader.close();
				}
			} catch (IOException e) {
				Logger.getLogger(this.getClass()).error(
						"IO error while closing positive words	readr", e);
			}
		}
	}

	private static PositiveWords get() {
		if (_singleton == null) {
			_singleton = new PositiveWords();
		}
		return _singleton;
	}

	public static Set<String> getWords() {
		return get().posWords;
	}
}
