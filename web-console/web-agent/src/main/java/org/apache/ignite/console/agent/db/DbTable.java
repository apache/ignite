

package org.apache.ignite.console.agent.db;

import java.util.Collection;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Database table.
 */
public class DbTable {
    /** Schema name. */
    private final String schema;

    /** Table name. */
    private final String tbl;
    
    private String comment;

    /** Columns. */
    private final Collection<DbColumn> cols;

    /** Indexes. */
    private final Collection<VisorQueryIndex> idxs;

    /**
     * Default columns.
     *
     * @param schema Schema name.
     * @param tbl Table name.
     * @param cols Columns.
     * @param idxs Indexes;
     */
    public DbTable(String schema, String tbl, String comment, Collection<DbColumn> cols, Collection<VisorQueryIndex> idxs) {
        this.schema = schema;
        this.tbl = tbl;
        this.comment = comment;
        this.cols = cols;
        this.idxs = idxs;
    }

    /**
     * @return Schema name.
     */
    public String getSchema() {
        return schema;
    }

    /**
     * @return Table name.
     */
    public String getTable() {
        return tbl;
    }

    /**
     * @return Columns.
     */
    public Collection<DbColumn> getColumns() {
        return cols;
    }

    /**
     * @return Indexes.
     */
    public Collection<VisorQueryIndex> getIndexes() {
        return idxs;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DbTable.class, this);
    }

	public String getComment() {
		return comment;
	}

	public void setComment(String comment) {
		this.comment = comment;
	}
}
