package org.apache.ignite.internal.processors.odbc.response;

import java.util.Collection;

/**
 * Query fetch result.
 */
public class QueryFetchResult {
    /** Query ID. */
    private long queryId;

    /** Query result rows. */
    private Collection<?> items = null;

    /** Flag indicating the query has no unfetched results. */
    private boolean last = false;

    /**
     * @param queryId Query ID.
     */
    public QueryFetchResult(long queryId){
        this.queryId = queryId;
    }

    /**
     * @return Query ID.
     */
    public long getQueryId() {
        return queryId;
    }

    /**
     * @param items Query result rows.
     */
    public void setItems(Collection<?> items) {
        this.items = items;
    }

    /**
     * @return Query result rows.
     */
    public Collection<?> getItems() {
        return items;
    }

    /**
     * @param last Flag indicating the query has no unfetched results.
     */
    public void setLast(boolean last) {
        this.last = last;
    }

    /**
     * @return Flag indicating the query has no unfetched results.
     */
    public boolean getLast() {
        return last;
    }
}
