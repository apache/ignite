/*
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.hawkore.ignite.lucene.index;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.AnalyzerWrapper;
import org.apache.lucene.analysis.FilteringTokenFilter;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.IndexWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.MoreObjects;

/** 
 * AnalyzerWrapper that discards too large tokens.
 *
 * @author Andres de la Pena `adelapena@stratio.com`
 * @author Manuel Núñez (manuel.nunez@hawkore.com)

 * @date 7 de jun. de 2017
 */

public class TokenLengthAnalyzer extends AnalyzerWrapper {

	/** Logger. */
	private static final Logger logger = LoggerFactory.getLogger(TokenLengthAnalyzer.class);
	
	private Analyzer analyzer;

	/**
	 * 
	 * @return the wrapped Analyzer
	 */
	public Analyzer analyzer(){
		return analyzer;
	}
	
	/**
	 * 
	 * @param analyzer the analyzer to be wrapped
	 */
	public TokenLengthAnalyzer(Analyzer analyzer) {
		super(analyzer.getReuseStrategy());
		this.analyzer=analyzer;
	}

	@Override
	protected Analyzer getWrappedAnalyzer(String arg0) {
		return analyzer;
	}


	/** @inheritdoc */
	@Override  
	protected TokenStreamComponents wrapComponents(String field, TokenStreamComponents components ) {

		FilteringTokenFilter tokenFilter = new FilteringTokenFilter(components.getTokenStream()) {

			CharTermAttribute term = addAttribute(CharTermAttribute.class);
			int maxSize = IndexWriter.MAX_TERM_LENGTH;

			/** @inheritdoc */
			@Override 
			protected boolean accept() {
				int size = term.length();
				if (size <= maxSize) {
					return true;
				}else {
					logger.warn("Discarding immense term in field='%s', Lucene only allows terms with at most %s bytes in length; got %s", field, maxSize, size);
					return false;
				}
			}
		};

		return new TokenStreamComponents(components.getSource(), tokenFilter);
	}

	@Override
	public String toString() {

		return MoreObjects.toStringHelper(this).add("analyzer", analyzer).toString();
	}

}




