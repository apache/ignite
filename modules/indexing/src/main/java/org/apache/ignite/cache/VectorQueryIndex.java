package org.apache.ignite.cache;

import org.apache.lucene.index.VectorSimilarityFunction;

import java.util.Collection;
import java.util.LinkedHashMap;


public class VectorQueryIndex extends QueryIndex {
	private static final long serialVersionUID = 0L;

	private String similarity;


	private int dimensions = 1024;

	/**
	 * Creates an empty index. Should be populated via setters.
	 */
	public VectorQueryIndex() {
		super.setIndexType(QueryIndexType.FULLTEXT);
	}

	/**
	 * Creates single-field vector index.
	 *
	 * @param field Field name.
	 *
	 */
	public VectorQueryIndex(String field,String similarity) {
		this(field,similarity,1024);
	}


    public VectorQueryIndex(String field,String similarity,int dimensions) {
    	super(field, QueryIndexType.FULLTEXT,false);
    	this.similarity = similarity;
		this.dimensions = dimensions;
    }


	public String getSimilarity() {
		return similarity;
	}

	public void setSimilarity(String similarity) {
		this.similarity = similarity;
	}

	public int getDimensions() {
		return dimensions;
	}

	public void setDimensions(int dimensions) {
		this.dimensions = dimensions;
	}
}
