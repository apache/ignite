package org.apache.ignite.internal.sql.command;

import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Index column definition.
 */
public class SqlIndexColumn extends SqlCommand {
    /** Column name. */
    private String name;

    /** Descending flag. */
    private boolean desc;

    /**
     * @return Column name.
     */
    public String name() {
        return name;
    }

    /**
     * @param name Column name.
     * @return This instance.
     */
    public SqlIndexColumn name(String name) {
        this.name = name;

        return this;
    }

    /**
     * @return Descending flag.
     */
    public boolean descending() {
        return desc;
    }

    /**
     * @param desc CDescending flag.
     * @return This instance.
     */
    public SqlIndexColumn descending(boolean desc) {
        this.desc = desc;

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SqlIndexColumn.class, this);
    }
}
