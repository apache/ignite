package org.apache.ignite.internal.processors.query.h2.views;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.h2.table.Column;
import org.h2.value.Value;

/**
 * Meta view base class.
 */
public abstract class SqlAbstractMetaView implements SqlMetaView {
    /** Default row count approximation. */
    protected static final long DEFAULT_ROW_COUNT_APPROXIMATION = 100L;

    /** Table name. */
    protected final String tblName;

    /** Description. */
    protected final String desc;

    /** Grid context. */
    protected final GridKernalContext ctx;

    /** Logger. */
    protected final IgniteLogger log;

    /** Columns. */
    protected final Column[] cols;

    /** Indexed column names. */
    protected final String[] indexes;

    /**
     * @param tblName Table name.
     * @param desc Descriptor.
     * @param ctx Context.
     * @param cols Columns.
     * @param indexes Indexes.
     */
    public SqlAbstractMetaView(String tblName, String desc, GridKernalContext ctx, Column[] cols,
        String[] indexes) {
        this.tblName = tblName;
        this.desc = desc;
        this.ctx = ctx;
        this.cols = cols;
        this.indexes = indexes;
        this.log = ctx.log(this.getClass());
    }

    /**
     * @param name Name.
     */
    protected static Column newColumn(String name) {
        return newColumn(name, Value.STRING);
    }

    /**
     * @param name Name.
     * @param type Type.
     */
    protected static Column newColumn(String name, int type) {
        return new Column(name, type);
    }

    /** {@inheritDoc} */
    @Override public String getTableName() {
        return tblName;
    }

    /** {@inheritDoc} */
    @Override public String getDescription() {
        return desc;
    }

    /** {@inheritDoc} */
    @Override public Column[] getColumns() {
        return cols;
    }

    /** {@inheritDoc} */
    @Override public String[] getIndexes() {
        return indexes;
    }

    /** {@inheritDoc} */
    @Override public long getRowCount() {
        return DEFAULT_ROW_COUNT_APPROXIMATION;
    }

    /** {@inheritDoc} */
    @Override public boolean canGetRowCount() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public String getCreateSQL() {
        StringBuilder sql = new StringBuilder();

        sql.append("CREATE TABLE " + getTableName() + '(');

        boolean isFirst = true;

        for (Column col : getColumns()) {
            if (isFirst)
                isFirst = false;
            else
                sql.append(", ");

            sql.append(col.getCreateSQL());
        }

        sql.append(')');

        return sql.toString();
    }
}
