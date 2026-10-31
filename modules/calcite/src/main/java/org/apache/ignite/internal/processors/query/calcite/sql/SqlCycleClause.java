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

package org.apache.ignite.internal.processors.query.calcite.sql;

import java.util.List;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.jetbrains.annotations.Nullable;

import static java.util.Objects.requireNonNull;

/**
 * SQL-standard cycle detection clause of a recursive common table expression, ported from CALCITE-7814.
 *
 * <p>Syntax: {@code CYCLE columns SET mark [TO value DEFAULT default] USING path}.
 * Omitted mark values are represented by TRUE and FALSE literals.
 * The mark and path columns are generated columns, not members of the WITH
 * item's explicit column list.
 */
public class SqlCycleClause extends SqlCall {
    // TODO: https://issues.apache.org/jira/browse/IGNITE-29207 Use Calcite's SqlCycleClause and SqlKind.CYCLE.
    /** CYCLE is syntax only; it is removed before conversion to relational operators. */
    private static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("CYCLE", SqlKind.OTHER) {
        /** {@inheritDoc} */
        @Override public SqlCall createCall(@Nullable SqlLiteral qualifier,
            SqlParserPos pos, @Nullable SqlNode... operands) {
            return new SqlCycleClause(pos,
                (SqlNodeList)requireNonNull(operands[0]),
                (SqlIdentifier)requireNonNull(operands[1]),
                requireNonNull(operands[2]), requireNonNull(operands[3]),
                (SqlIdentifier)requireNonNull(operands[4]));
        }
    };

    /** Columns identifying a vertex in each path. */
    public SqlNodeList columns;

    /** Name of the generated cycle mark column. */
    public SqlIdentifier markColumn;

    /** Value emitted for the row closing a cycle. */
    public SqlNode markValue;

    /** Value emitted for other rows. */
    public SqlNode defaultValue;

    /** Name of the generated ARRAY[ROW] path column. */
    public SqlIdentifier pathColumn;

    /** */
    public SqlCycleClause(SqlParserPos pos, SqlNodeList columns,
        SqlIdentifier markColumn, SqlNode markValue, SqlNode defaultValue, SqlIdentifier pathColumn) {
        super(pos);

        this.columns = columns;
        this.markColumn = markColumn;
        this.markValue = markValue;
        this.defaultValue = defaultValue;
        this.pathColumn = pathColumn;
    }

    /** {@inheritDoc} */
    @Override public SqlOperator getOperator() {
        return OPERATOR;
    }

    /** {@inheritDoc} */
    @Override public List<SqlNode> getOperandList() {
        return ImmutableList.of(columns, markColumn, markValue, defaultValue, pathColumn);
    }

    /** {@inheritDoc} */
    @Override public void setOperand(int i, @Nullable SqlNode operand) {
        requireNonNull(operand, "operand");

        switch (i) {
            case 0:
                columns = (SqlNodeList)operand;
                break;
            case 1:
                markColumn = (SqlIdentifier)operand;
                break;
            case 2:
                markValue = operand;
                break;
            case 3:
                defaultValue = operand;
                break;
            case 4:
                pathColumn = (SqlIdentifier)operand;
                break;
            default:
                throw new AssertionError(i);
        }
    }

    /** {@inheritDoc} */
    @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("CYCLE");
        final SqlWriter.Frame frame = writer.startList("", "");
        for (SqlNode column : columns) {
            writer.sep(",");
            column.unparse(writer, 0, 0);
        }
        writer.endList(frame);
        writer.keyword("SET");
        markColumn.unparse(writer, 0, 0);
        writer.keyword("TO");
        markValue.unparse(writer, 0, 0);
        writer.keyword("DEFAULT");
        defaultValue.unparse(writer, 0, 0);
        writer.keyword("USING");
        pathColumn.unparse(writer, 0, 0);
    }
}
