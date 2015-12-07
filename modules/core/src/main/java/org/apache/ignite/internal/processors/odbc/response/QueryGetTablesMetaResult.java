package org.apache.ignite.internal.processors.odbc.response;

import org.apache.ignite.internal.processors.odbc.GridOdbcTableMeta;

import java.util.Collection;

/**
 * Query get columns meta result.
 */
public class QueryGetTablesMetaResult {
    /** Query result rows. */
    private Collection<GridOdbcTableMeta> meta;

    /**
     * @param meta Column metadata.
     */
    public QueryGetTablesMetaResult(Collection<GridOdbcTableMeta> meta) {
        this.meta = meta;
    }

    /**
     * @return Query result rows.
     */
    public Collection<GridOdbcTableMeta> getMeta() {
        return meta;
    }
}
