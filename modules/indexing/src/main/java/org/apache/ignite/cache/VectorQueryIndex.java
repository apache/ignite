package org.apache.ignite.cache;

import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.index.VectorEncoding;
import java.util.Collection;
import java.util.LinkedHashMap;


public class VectorQueryIndex extends QueryIndex {
	private static final long serialVersionUID = 0L;

	private VectorSimilarityFunction similarity;
	private VectorEncoding encoding = VectorEncoding.FLOAT32;
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
	public VectorQueryIndex(String field,VectorSimilarityFunction similarity) {
		this(field,similarity,1024);
	}


    public VectorQueryIndex(String field,VectorSimilarityFunction similarity,int dimensions) {
    	super(field, QueryIndexType.FULLTEXT,false);
    	this.similarity = similarity;
		this.dimensions = dimensions;
    }

	public VectorQueryIndex(String field,VectorSimilarityFunction similarity,int dimensions,VectorEncoding dataType) {
		super(field, QueryIndexType.FULLTEXT,false);
		this.similarity = similarity;
		this.dimensions = dimensions;
		this.encoding = dataType;
	}


	public VectorSimilarityFunction getSimilarity() {
		return similarity;
	}

	public void setSimilarity(VectorSimilarityFunction similarity) {
		this.similarity = similarity;
	}

	public int getDimensions() {
		return dimensions;
	}

	public void setDimensions(int dimensions) {
		this.dimensions = dimensions;
	}


	public VectorEncoding getEncoding() {
		return encoding;
	}

	public void setEncoding(VectorEncoding encoding) {
		this.encoding = encoding;
	}
}
