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
import org.apache.ignite.internal.util.typedef.F;
import org.h2.command.Parser;
import org.h2.expression.Expression;
import org.h2.table.Column;

/**
 * Column.
 */
public class GridSqlColumn extends GridSqlElement {
    /** */
    private GridSqlAst from;

    /** */
    private String schema;

    /** */
    private String tblAlias;

    /** */
    private String colName;

    /** */
    private Column col;

    /**
     * @param col Column.
     * @param from From.
     * @param colName Column name.
     */
    public GridSqlColumn(Column col, GridSqlAst from, String colName) {
        this(col, from, null, null, colName);
    }

    /**
     * @param col Column.
     * @param from From.
     * @param schema Schema name.
     * @param tblAlias Table alias.
     * @param colName Column name.
     */
    public GridSqlColumn(Column col, GridSqlAst from, String schema, String tblAlias, String colName) {
        super(Collections.<GridSqlAst>emptyList());

        assert !F.isEmpty(colName): colName;

        this.col = col;
        this.from = from;

        this.colName = colName;
        this.schema = schema;
        this.tblAlias = tblAlias;
    }

    /**
     * @return Column name.
     */
    public String columnName() {
        return colName;
    }

    /**
     * @return Schema name.
     */
    public String schema() {
        return schema;
    }

    /**
     * @param tblAlias Table alias.
     */
    public void tableAlias(String tblAlias) {
        this.tblAlias = tblAlias;
    }

    /**
     * @return Table alias.
     */
    public String tableAlias() {
        return tblAlias;
    }

    /** {@inheritDoc} */
    @Override public String getSQL() {
        String sql = Parser.quoteIdentifier(colName);

        if (tblAlias != null)
            sql = Parser.quoteIdentifier(tblAlias) + "." + sql;

        if (schema != null)
            sql = Parser.quoteIdentifier(schema) + "." + sql;

        return sql;
    }

    /**
     * @return Expression in from.
     */
    public GridSqlAst expressionInFrom() {
        return from;
    }

    /**
     * @param from Expression in from.
     */
    public void expressionInFrom(GridSqlAlias from) {
        this.from = from;
    }

    /**
     * @return Default value.
     */
    public Object defaultValue() {
        Expression dfltExpr = col.getDefaultExpression();

        return dfltExpr != null ? col.convert(dfltExpr.getValue(null)).getObject() : null;
    }

    /**
     * @return Precision.
     */
    public int precision() {
        return (int) col.getPrecision();
    }

    /**
     * @return Scale.
     */
    public int scale() {
        return col.getScale();
    }

    /**
     * @return H2 Column.
     */
    public Column column() {
        return col;
    }
}
