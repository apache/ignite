package org.apache.ignite.cache;

public class FullTextQueryIndex extends QueryIndex {

    /**
     * Creates an empty index. Should be populated via setters.
     */
    public FullTextQueryIndex() {
        super.setIndexType(QueryIndexType.FULLTEXT);
    }

    /**
     * Creates single-field sorted ascending index.
     *
     * @param field Field name.
     */
    public FullTextQueryIndex(String field) {
        super(field, QueryIndexType.FULLTEXT);
    }

}
