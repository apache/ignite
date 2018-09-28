package org.apache.ignite.cache;

import org.apache.lucene.analysis.Analyzer;

public class FullTextQueryIndex extends QueryIndex {
	private Analyzer analyzer;

	private Analyzer queryAnalyzer;

	/**
	 * Creates an empty index. Should be populated via setters.
	 */
	public FullTextQueryIndex() {
		super.setIndexType(QueryIndexType.FULLTEXT);
	}

	/**
	 * Creates single-field sorted ascending index.
	 *
	 * @param field
	 *            Field name.
	 */
	public FullTextQueryIndex(String field) {
		super(field, QueryIndexType.FULLTEXT);
	}

	/**
	 * Creates single-field sorted ascending index.
	 *
	 * @param field
	 *            Field name.
	 */
	public FullTextQueryIndex(String field, Analyzer analyzer) {
		super(field, QueryIndexType.FULLTEXT);
		this.analyzer = analyzer;
		this.setQueryAnalyzer(analyzer);
	}
	
	/**
	 * Creates single-field sorted ascending index.
	 *
	 * @param field
	 *            Field name.
	 */
	public FullTextQueryIndex(String field, Analyzer analyzer,Analyzer queryAnalyzer) {
		super(field, QueryIndexType.FULLTEXT);
		this.analyzer = analyzer;
		this.setQueryAnalyzer(queryAnalyzer);
	}

	public Analyzer getAnalyzer() {
		return analyzer;
	}

	public void setAnalyzer(Analyzer analyzer) {
		this.analyzer = analyzer;
	}

	public Analyzer getQueryAnalyzer() {
		return queryAnalyzer;
	}

	public void setQueryAnalyzer(Analyzer queryAnalyzer) {
		this.queryAnalyzer = queryAnalyzer;
	}
}
