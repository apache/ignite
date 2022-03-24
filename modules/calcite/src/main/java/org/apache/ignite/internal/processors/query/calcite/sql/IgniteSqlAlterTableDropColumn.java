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
 * Parse tree for {@code ALTER TABLE ... DROP COLUMN} statement
 */
public class IgniteSqlAlterTableDropColumn extends IgniteAbstractSqlAlterTable {
    /** */
    private final boolean ifExistsColumn;

    /** */
    private final SqlNodeList columns;

    /** */
    public IgniteSqlAlterTableDropColumn(SqlParserPos pos, boolean ifExists, SqlIdentifier tblName,
        boolean ifExistsColumn, SqlNodeList columns) {
        super(pos, ifExists, tblName);
        this.ifExistsColumn = ifExistsColumn;
        this.columns = Objects.requireNonNull(columns, "columns list");
    }

    /** {@inheritDoc} */
    @Override public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(name, columns);
    }

    /** {@inheritDoc} */
    @Override protected void unparseAlterTableOperation(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("DROP");
        writer.keyword("COLUMN");

        if (ifExistsColumn)
            writer.keyword("IF EXISTS");

        columns.unparse(writer, leftPrec, rightPrec);
    }

    /**
     * @return Whether the IF EXISTS is specified for columns.
     */
    public boolean ifExistsColumn() {
        return ifExistsColumn;
    }

    /**
     * @return Columns list.
     */
    public SqlNodeList columns() {
        return columns;
    }
}
