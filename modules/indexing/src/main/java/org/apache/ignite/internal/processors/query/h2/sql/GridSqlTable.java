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

package org.apache.ignite.internal.processors.query.h2.sql;

import java.util.Collections;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
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
    private boolean affKeyCond;

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
        super(Collections.<GridSqlElement>emptyList());

        assert schema != null : "schema";
        assert tblName != null : "tblName";

        this.schema = schema;
        this.tblName = tblName;

        this.tbl = tbl instanceof GridH2Table ? (GridH2Table)tbl : null;
    }

    /**
     * @param affKeyCond If affinity key condition is found.
     */
    public void affinityKeyCondition(boolean affKeyCond) {
        this.affKeyCond = affKeyCond;
    }

    /**
     * @return {@code true} If affinity key condition is found.
     */
    public boolean affinityKeyCondition() {
        return affKeyCond;
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