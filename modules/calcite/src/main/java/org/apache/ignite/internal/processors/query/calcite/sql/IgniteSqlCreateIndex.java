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
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;
import org.jetbrains.annotations.Nullable;

/**
 * Parse tree for {@code CREATE INDEX} statement
 */
public class IgniteSqlCreateIndex extends SqlCreate {
    /** */
    private final SqlIdentifier idxName;

    /** */
    private final SqlIdentifier tblName;

    /** */
    private final SqlNodeList columnList;

    /** */
    private final SqlNumericLiteral parallel;

    /** */
    private final SqlNumericLiteral inlineSize;

    /** */
    private static final SqlOperator OPERATOR =
        new SqlSpecialOperator("CREATE INDEX", SqlKind.CREATE_INDEX);

    /** Creates a SqlCreateIndex. */
    public IgniteSqlCreateIndex(
        SqlParserPos pos,
        boolean ifNotExists,
        @Nullable SqlIdentifier idxName,
        SqlIdentifier tblName,
        SqlNodeList columnList,
        SqlNumericLiteral parallel,
        SqlNumericLiteral inlineSize
    ) {
        super(OPERATOR, pos, false, ifNotExists);
        this.idxName = idxName;
        this.tblName = Objects.requireNonNull(tblName, "table name");
        this.columnList = columnList;
        this.parallel = parallel;
        this.inlineSize = inlineSize;
    }

    /** {@inheritDoc} */
    @Override public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(idxName, tblName, columnList, parallel, inlineSize);
    }

    /** {@inheritDoc} */
    @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("CREATE");

        writer.keyword("INDEX");

        if (ifNotExists)
            writer.keyword("IF NOT EXISTS");

        if (idxName != null)
            idxName.unparse(writer, 0, 0);

        writer.keyword("ON");

        tblName.unparse(writer, 0, 0);

        SqlWriter.Frame frame = writer.startList("(", ")");

        for (SqlNode c : columnList) {
            writer.sep(",");

            boolean desc = false;

            if (c.getKind() == SqlKind.DESCENDING) {
                c = ((SqlCall)c).getOperandList().get(0);
                desc = true;
            }

            c.unparse(writer, 0, 0);

            if (desc)
                writer.keyword("DESC");
        }

        writer.endList(frame);

        if (parallel != null) {
            writer.keyword("PARALLEL");

            parallel.unparse(writer, 0, 0);
        }

        if (inlineSize != null) {
            writer.keyword("INLINE_SIZE");

            inlineSize.unparse(writer, 0, 0);
        }
    }

    /**
     * @return Name of the index.
     */
    public SqlIdentifier indexName() {
        return idxName;
    }

    /**
     * @return Name of the table.
     */
    public SqlIdentifier tableName() {
        return tblName;
    }

    /**
     * @return List of the specified columns and constraints.
     */
    public SqlNodeList columnList() {
        return columnList;
    }

    /**
     * @return PARALLEL clause.
     */
    public SqlNumericLiteral parallel() {
        return parallel;
    }

    /**
     * @return INLINE_SIZE clause.
     */
    public SqlNumericLiteral inlineSize() {
        return inlineSize;
    }

    /**
     * @return Whether the IF NOT EXISTS is specified.
     */
    public boolean ifNotExists() {
        return ifNotExists;
    }
}
