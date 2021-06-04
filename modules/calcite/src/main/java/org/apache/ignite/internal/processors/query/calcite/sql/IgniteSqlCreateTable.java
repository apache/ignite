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

import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Parse tree for {@code CREATE TABLE} statement with Ignite specific features.
 */
public class IgniteSqlCreateTable extends SqlCreate {
    /** */
    private final SqlIdentifier name;

    /** */
    private final @Nullable SqlNodeList columnList;

    /** */
    private final @Nullable SqlNodeList createOptionList;

    /** */
    private static final SqlOperator OPERATOR =
        new SqlSpecialOperator("CREATE TABLE", SqlKind.CREATE_TABLE);

    /** Creates a SqlCreateTable. */
    public IgniteSqlCreateTable(SqlParserPos pos, boolean ifNotExists,
        SqlIdentifier name, @Nullable SqlNodeList columnList, @Nullable SqlNodeList createOptionList) {
        super(OPERATOR, pos, false, ifNotExists);
        this.name = Objects.requireNonNull(name, "name");
        this.columnList = columnList;
        this.createOptionList = createOptionList;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("nullness")
    @Override public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(name, columnList, createOptionList);
    }

    /** {@inheritDoc} */
    @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("CREATE");
        writer.keyword("TABLE");
        if (ifNotExists)
            writer.keyword("IF NOT EXISTS");

        name.unparse(writer, leftPrec, rightPrec);
        if (columnList != null) {
            SqlWriter.Frame frame = writer.startList("(", ")");
            for (SqlNode c : columnList) {
                writer.sep(",");
                c.unparse(writer, 0, 0);
            }
            writer.endList(frame);
        }

        if (createOptionList != null) {
            writer.keyword("WITH");

            createOptionList.unparse(writer, 0, 0);
        }
    }

    /**
     * @return Name of the table.
     */
    public SqlIdentifier name() {
        return name;
    }

    /**
     * @return List of the specified columns and constraints.
     */
    public SqlNodeList columnList() {
        return columnList;
    }

    /**
     * @return List of the specified options to create table with.
     */
    public SqlNodeList createOptionList() {
        return createOptionList;
    }

    /**
     * @return Whether the IF NOT EXISTS is specified.
     */
    public boolean ifNotExists() {
        return ifNotExists;
    }
}
