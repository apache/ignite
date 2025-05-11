package org.apache.ignite.cache;

import java.util.Collection;
import java.util.LinkedHashMap;


public class FullTextQueryIndex extends QueryIndex {
	private static final long serialVersionUID = 0L;
	
	private String analyzer;

	private String queryAnalyzer;

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
		this(field,null,null);
	}
	

    /**
     * Creates text index for a collection of fields. If index is sorted, fields will be sorted in
     * ascending order.
     *
     * @param fields Collection of fields to create an index.
     * @param type Index type.
     */
    public FullTextQueryIndex(Collection<String> fields, String analyzer) {
    	super(fields, QueryIndexType.FULLTEXT);
    	this.analyzer = analyzer;
		this.setQueryAnalyzer(analyzer);
    }
    
    public FullTextQueryIndex(Collection<String> fields) {
    	super(fields, QueryIndexType.FULLTEXT);    	
    }


	/**
	 * Creates single-field sorted ascending index.
	 *
	 * @param field
	 *            Field name.
	 */
	public FullTextQueryIndex(String fieldList, String analyzer) {
		this(fieldList,analyzer,analyzer);
	}
	
	/**
	 * Creates single-field sorted ascending index.
	 *
	 * @param field
	 *            Field name.
	 */
	public FullTextQueryIndex(String fieldList, String analyzer,String queryAnalyzer) {
		String [] fields = fieldList.split("[,\\s]");
		LinkedHashMap<String,Boolean> fieldsList = new LinkedHashMap<>();
        for (String f : fields)
        	fieldsList.put(f, true);
		this.setFields(fieldsList);
		this.analyzer = analyzer;
		this.setQueryAnalyzer(queryAnalyzer);
		super.setIndexType(QueryIndexType.FULLTEXT);
	}

	public String getAnalyzer() {
		return analyzer;
	}

	public void setAnalyzer(String analyzer) {
		this.analyzer = analyzer;
	}

	public String getQueryAnalyzer() {
		return queryAnalyzer;
	}

	public void setQueryAnalyzer(String queryAnalyzer) {
		this.queryAnalyzer = queryAnalyzer;
	}
}
