package org.apache.ignite.internal.processors.odbc.request;

/**
 * ODBC query fetch request.
 */
public class QueryFetchRequest extends GridOdbcRequest {
    /** Query ID. */
    private long queryId;

    /** Page size - maximum number of rows to return. */
    private Integer pageSize;

    /**
     * @param queryId Query ID.
     * @param pageSize Page size.
     */
    public QueryFetchRequest(long queryId, int pageSize) {
        super(FETCH_SQL_QUERY);
        this.queryId = queryId;
        this.pageSize = pageSize;
    }

    /**
     * @param pageSize Page size.
     */
    public void pageSize(Integer pageSize) {
        this.pageSize = pageSize;
    }

    /**
     * @return Page size.
     */
    public int pageSize() {
        return pageSize;
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