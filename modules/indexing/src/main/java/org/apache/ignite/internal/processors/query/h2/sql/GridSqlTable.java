/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.query.h2.sql;

import java.util.Collections;
import java.util.List;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.h2.command.Parser;
import org.h2.table.Table;
import org.jetbrains.annotations.Nullable;

/**
 * Table with optional schema.
 */
public class GridSqlTable extends GridSqlElement {
    /** */
    private final String schema;

    /** */
    private final String tblName;

    /** */
    private final GridH2Table tbl;

    /** */
    private List<String> useIndexes;

    /**
     * @param schema Schema.
     * @param tblName Table name.
     */
    public GridSqlTable(@Nullable String schema, String tblName) {
        this(schema, tblName, null);
    }

    /**
     * @param tbl Table.
     */
    public GridSqlTable(Table tbl) {
        this(tbl.getSchema().getName(), tbl.getName(), tbl);
    }

    /**
     * @param schema Schema.
     * @param tblName Table name.
     * @param tbl H2 Table.
     */
    private GridSqlTable(@Nullable String schema, String tblName, @Nullable Table tbl) {
        super(Collections.<GridSqlAst>emptyList());

        assert schema != null : "schema";
        assert tblName != null : "tblName";

        this.schema = schema;
        this.tblName = tblName;

        this.tbl = tbl instanceof GridH2Table ? (GridH2Table)tbl : null;
    }

    /** {@inheritDoc} */
    @Override public String getSQL() {
        return getBeforeAliasSql() + getAfterAliasSQL();
    }

    /**
     * @return SQL for the table before alias.
     */
    public String getBeforeAliasSql() {
        if (schema == null)
            return Parser.quoteIdentifier(tblName);

        return Parser.quoteIdentifier(schema) + '.' + Parser.quoteIdentifier(tblName);
    }

    /**
     * @return SQL for the table after alias.
     */
    public String getAfterAliasSQL() {
        if (useIndexes == null)
            return "";

        SB b = new SB();

        b.a(" USE INDEX (");

        boolean first = true;

        for (String idx : useIndexes) {
            if (first)
                first = false;
            else
                b.a(", ");

            b.a(Parser.quoteIdentifier(idx));
        }

        b.a(')');

        return b.toString();
    }

    /**
     * @param useIndexes List of indexes.
     */
    public void useIndexes(List<String> useIndexes) {
        this.useIndexes = useIndexes;
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

    /**
     * @return Referenced data table.
     */
    public GridH2Table dataTable() {
        return tbl;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (!super.equals(o))
            return false;

        GridSqlTable that = (GridSqlTable)o;

        return schema.equals(that.schema) && tblName.equals(that.tblName);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int result = 1;

        result = 31 * result + schema.hashCode();
        result = 31 * result + tblName.hashCode();

        return result;
    }
}