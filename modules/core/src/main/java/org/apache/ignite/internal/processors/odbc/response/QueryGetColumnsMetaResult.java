package org.apache.ignite.internal.processors.odbc.response;

import org.apache.ignite.internal.processors.odbc.GridOdbcColumnMeta;

import java.util.Collection;

/**
 * Query get columns meta result.
 */
public class QueryGetColumnsMetaResult {
    /** Query result rows. */
    private Collection<GridOdbcColumnMeta> meta;

    /**
     * @param meta Column metadata.
     */
    public QueryGetColumnsMetaResult(Collection<GridOdbcColumnMeta> meta) {
        this.meta = meta;
    }

    /**
     * @return Query result rows.
     */
    public Collection<GridOdbcColumnMeta> getMeta() {
        return meta;
    }
}
