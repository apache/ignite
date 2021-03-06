/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.h2.sys.view;

import java.util.Iterator;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.h2.engine.Session;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.table.Column;
import org.h2.value.Value;

import static org.apache.ignite.plugin.security.SecurityPermission.SYSTEM_VIEW_READ;

/**
 * Meta view base class.
 */
public abstract class SqlAbstractSystemView implements SqlSystemView {
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
    public SqlAbstractSystemView(String tblName, String desc, GridKernalContext ctx, Column[] cols,
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
    @Override public final Iterator<Row> getRows(Session ses, SearchRow first, SearchRow last) {
        authorize();

        return getRowsNoAuth(ses, first, last);
    }

    /**
     * {@link SqlSystemView#getRows(Session, SearchRow, SearchRow)} implementation without authorization.
     */
    protected abstract Iterator<Row> getRowsNoAuth(Session ses, SearchRow first, SearchRow last);

    /** {@inheritDoc} */
    @Override public final long getRowCount() {
        authorize();

        return getRowCountNoAuth();
    }

    /**
     * {@link SqlSystemView#getRowCount()} implementation without authorization.
     */
    protected long getRowCountNoAuth() {
        return DEFAULT_ROW_COUNT_APPROXIMATION;
    }

    /** {@inheritDoc} */
    @Override public final long getRowCountApproximation() {
        authorize();

        return getRowCountNoAuth();
    }

    /**
     * {@link SqlSystemView#getRowCountApproximation()} implementation without authorization.
     */
    protected long getRowCountApproximationNoAuth() {
        return DEFAULT_ROW_COUNT_APPROXIMATION;
    }

    /** {@inheritDoc} */
    @Override public boolean canGetRowCount() {
        return false;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("StringConcatenationInsideStringBufferAppend")
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

    /**
     * Authorizes {@link SecurityPermission#SYSTEM_VIEW_READ} permission.
     */
    private void authorize() {
        ctx.security().authorize(SYSTEM_VIEW_READ);
    }
}
