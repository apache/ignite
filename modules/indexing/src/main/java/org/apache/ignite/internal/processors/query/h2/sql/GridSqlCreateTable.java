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

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import org.apache.ignite.internal.util.GridStringBuilder;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.h2.command.Parser;

/**
 * CREATE TABLE statement.
 */
public class GridSqlCreateTable extends GridSqlStatement {
    /**
     * Schema name upon which this statement has been issued - <b>not</b> the name of the schema where this new table
     * will be created. */
    private String schemaName;

    /** Table name. */
    private String tblName;

    /** Cache name upon which new cache configuration for this table must be based. */
    private String tplCacheName;

    /** Quietly ignore this command if table already exists. */
    private boolean ifNotExists;

    /** Columns. */
    private LinkedHashMap<String, GridSqlColumn> cols;

    /** Primary key columns. */
    private LinkedHashSet<String> pkCols;

    /** Extra WITH-params. */
    private List<String> params;

    public String templateCacheName() {
        return tplCacheName;
    }

    public void templateCacheName(String tplCacheName) {
        this.tplCacheName = tplCacheName;
    }

    public LinkedHashMap<String, GridSqlColumn> columns() {
        return cols;
    }

    public void columns(LinkedHashMap<String, GridSqlColumn> cols) {
        this.cols = cols;
    }

    public LinkedHashSet<String> primaryKeyColumns() {
        return pkCols;
    }

    public void primaryKeyColumns(LinkedHashSet<String> pkCols) {
        this.pkCols = pkCols;
    }

    public String schemaName() {
        return schemaName;
    }

    public void schemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String tableName() {
        return tblName;
    }

    public void tableName(String tblName) {
        this.tblName = tblName;
    }

    public boolean ifNotExists() {
        return ifNotExists;
    }

    public void ifNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    public List<String> params() {
        return params;
    }

    public void params(List<String> params) {
        this.params = params;
    }

    /** {@inheritDoc} */
    @Override public String getSQL() {
        GridStringBuilder b = new SB("CREATE TABLE ")
            .a(ifNotExists ? "IF NOT EXISTS " : "")
            .a("\n")
            .a(Parser.quoteIdentifier(schemaName))
            .a('.')
            .a(Parser.quoteIdentifier(tblName))
            .a("\n(");

        boolean singleColPk = false;

        boolean first = true;

        for (GridSqlColumn col : cols.values()) {
            if (!first)
                b.a(",\n");
            else
                first = false;

            if (col.column().isPrimaryKey()) {
                // Only one column may be marked PRIMARY KEY - multi-col PK is defined separately
                assert !singleColPk;

                singleColPk = true;
            }

            b.a('\t')
                .a(col.getSQL())
                .a(' ')
                .a(col.resultType().sql())
                .a(col.column().isPrimaryKey() ? " PRIMARY KEY" : "");
        }

        first = true;

        if (!singleColPk && !F.isEmpty(pkCols)) {
            b.a(",\n")
                .a('\t')
                .a("PRIMARY KEY (\n");

            for (String col : pkCols) {
                GridSqlColumn pkCol = cols.get(col);

                assert pkCol != null;

                if (!first)
                    b.a(",\n");
                else
                    first = false;

                b.a("\t\t")
                    .a(pkCol.getSQL());
            }

            b.a("\n\t)");
        }

        b.a("\n)");

        if (!F.isEmpty(params)) {
            b.a("\nWITH ");

            first = true;

            for (String p : params) {
                if (!first)
                    b.a(',');
                else
                    first = false;

                b.a(Parser.quoteIdentifier(p));
            }
        }

        return b.toString();
    }
}
