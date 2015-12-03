package org.apache.ignite.internal.processors.odbc.response;

/**
 * Query result.
 */
public class QueryCloseResult {
    /** Query ID. */
    private long queryId;

    /**
     * @param queryId Query ID.
     */
    public QueryCloseResult(long queryId){
        this.queryId = queryId;
    }

    /**
     * @return Query ID.
     */
    public long getQueryId() {
        return queryId;
    }
}
