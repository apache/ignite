package org.apache.ignite.internal.processors.odbc.request;

/**
 * ODBC query close request.
 */
public class QueryCloseRequest extends GridOdbcRequest {
    /** Query ID. */
    private long queryId;

    /**
     * @param queryId Query ID.
     */
    public QueryCloseRequest(long queryId) {
        super(CLOSE_SQL_QUERY);
        this.queryId = queryId;
    }

    /**
     * @param queryId Query ID.
     */
    public void cacheName(long queryId) {
        this.queryId = queryId;
    }

    /**
     * @return Query ID.
     */
    public long queryId() {
        return queryId;
    }
}