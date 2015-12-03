package org.apache.ignite.internal.processors.odbc.response;

import org.apache.ignite.internal.processors.odbc.GridOdbcColumnMeta;

import java.util.Collection;

/**
 * Query execute result.
 */
public class QueryExecuteResult {
    /** Query ID. */
    private long queryId;

    /** Fields metadata. */
    private Collection<GridOdbcColumnMeta> columnsMeta;

    /**
     * @param queryId Query ID.
     * @param columnsMeta Columns metadata.
     */
    public QueryExecuteResult(long queryId,Collection<GridOdbcColumnMeta> columnsMeta){
        this.queryId = queryId;
        this.columnsMeta = columnsMeta;
    }

    /**
     * @return Query ID.
     */
    public long getQueryId() {
        return queryId;
    }

    /**
     * @return Columns metadata.
     */
    public Collection<GridOdbcColumnMeta> getColumnsMetadata() {
        return columnsMeta;
    }
}
