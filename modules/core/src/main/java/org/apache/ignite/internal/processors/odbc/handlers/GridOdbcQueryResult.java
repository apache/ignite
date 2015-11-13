package org.apache.ignite.internal.processors.odbc.handlers;

/**
 * Query result.
 */
public class GridOdbcQueryResult {
    /** Query ID. */
    private long queryId;

    /**
     * @param queryId Query ID.
     */
    public GridOdbcQueryResult(long queryId){
        this.queryId = queryId;
    }

    /**
     * @return Query ID.
     */
    public long getQueryId() {
        return queryId;
    }
}
