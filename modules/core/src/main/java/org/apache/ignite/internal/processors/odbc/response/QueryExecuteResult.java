package org.apache.ignite.internal.processors.odbc.response;

import java.util.Collection;

/**
 * Query execute result.
 */
public class QueryExecuteResult {
    /** Query ID. */
    private long queryId;

    /** Fields metadata. */
    private Collection<?> fieldsMeta;

    /**
     * @param queryId Query ID.
     */
    public QueryExecuteResult(long queryId, Collection<?> fieldsMeta){
        this.queryId = queryId;
        this.fieldsMeta = fieldsMeta;
    }

    /**
     * @return Query ID.
     */
    public long getQueryId() {
        return queryId;
    }

    /**
     * @return Fields metadata.
     */
    public Collection<?> getFieldsMetadata() {
        return fieldsMeta;
    }
}
