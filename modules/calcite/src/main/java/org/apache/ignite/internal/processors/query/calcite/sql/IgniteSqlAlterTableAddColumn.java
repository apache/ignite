/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.processors.query.calcite.sql;

import java.util.List;
import java.util.Objects;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

/**
 * Parse tree for {@code ALTER TABLE ... ADD COLUMN} statement
 */
public class IgniteSqlAlterTableAddColumn extends IgniteAbstractSqlAlterTable {
    /** */
    private final boolean ifNotExistsColumn;

    /** */
    private final SqlNodeList columns;

    /** */
    public IgniteSqlAlterTableAddColumn(SqlParserPos pos, boolean ifExists, SqlIdentifier tblName,
        boolean ifNotExistsColumn, SqlNodeList columns) {
        super(pos, ifExists, tblName);
        this.ifNotExistsColumn = ifNotExistsColumn;
        this.columns = Objects.requireNonNull(columns, "columns list");
    }

    /** {@inheritDoc} */
    @Override public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(name, columns);
    }

    /** {@inheritDoc} */
    @Override protected void unparseAlterTableOperation(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("ADD");
        writer.keyword("COLUMN");

        if (ifNotExistsColumn)
            writer.keyword("IF NOT EXISTS");

        columns.unparse(writer, leftPrec, rightPrec);
    }

    /**
     * @return Whether the IF NOT EXISTS is specified for columns.
     */
    public boolean ifNotExistsColumn() {
        return ifNotExistsColumn;
    }

    /**
     * @return Columns list.
     */
    public SqlNodeList columns() {
        return columns;
    }
}
