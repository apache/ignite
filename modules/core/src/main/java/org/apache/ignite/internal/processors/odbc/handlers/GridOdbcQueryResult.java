package org.apache.ignite.internal.processors.odbc.handlers;

import java.util.Collection;
import java.util.List;

/**
 * Query result.
 */
public class GridOdbcQueryResult {
    /** Query ID. */
    private long queryId;

    /** Query result rows. */
    private Collection<?> items = null;

    /** Fields metadata. */
    private Collection<?> fieldsMeta;

    /** Flag indicating the query has no unfetched results. */
    private boolean last = false;

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

    /**
     * @param items Query result rows.
     */
    public void setItems(List<Object> items) {
        this.items = items;
    }

    /**
     * @param fieldsMeta Fields metadata.
     */
    public void setFieldsMetadata(Collection<?> fieldsMeta) {
        this.fieldsMeta = fieldsMeta;
    }

    /**
     * @return Fields metadata.
     */
    public Collection<?> getFieldsMetadata() {
        return fieldsMeta;
    }

    /**
     * @return Query result rows.
     */
    public Collection<?> getItems() {
        return items;
    }

    /**
     * @return Flag indicating the query has no unfetched results.
     */
    public boolean getLast() {
        return last;
    }

    /**
     * @param last Flag indicating the query has no unfetched results.
     */
    public void setLast(boolean last) {
        this.last = last;
    }
}
