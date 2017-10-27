package org.apache.ignite.internal.sql.command;

import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Index column definition.
 */
public class SqlIndexColumn {
    /** Column name. */
    private final String name;

    /** Descending flag. */
    private final boolean desc;

    /**
     * Constructor.
     *
     * @param name Column name.
     * @param desc Descending flag.
     */
    public SqlIndexColumn(String name, boolean desc) {
        this.name = name;
        this.desc = desc;
    }

    /**
     * @return Column name.
     */
    public String name() {
        return name;
    }

    /**
     * @return Descending flag.
     */
    public boolean descending() {
        return desc;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SqlIndexColumn.class, this);
    }
}
