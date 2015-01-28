/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.internal.processors.query.h2.sql;

import org.h2.command.*;
import org.jetbrains.annotations.*;

/**
 * Table with optional schema.
 */
public class GridSqlTable extends GridSqlElement {
    /** */
    private final String schema;

    /** */
    private final String tblName;

    /**
     * @param schema Schema.
     * @param tblName Table name.
     */
    public GridSqlTable(@Nullable String schema, String tblName) {
        this.schema = schema;
        this.tblName = tblName;
    }

    /** {@inheritDoc} */
    @Override public String getSQL() {
        if (schema == null)
            return Parser.quoteIdentifier(tblName);

        return Parser.quoteIdentifier(schema) + '.' + Parser.quoteIdentifier(tblName);
    }

    /**
     * @return Schema.
     */
    public String schema() {
        return schema;
    }

    /**
     * @return Table name.
     */
    public String tableName() {
        return tblName;
    }
}
