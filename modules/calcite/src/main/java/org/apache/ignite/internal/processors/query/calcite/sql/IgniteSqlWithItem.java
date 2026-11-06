/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;
import org.jetbrains.annotations.Nullable;

/** WITH item retaining its CYCLE clause until validation and rewriting. */
public class IgniteSqlWithItem extends SqlWithItem {
    // TODO: https://issues.apache.org/jira/browse/IGNITE-29207 Use Calcite's SqlWithItem after upgrading.
    /** Operator preserving the extra operand when syntax trees are copied. */
    private static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("WITH_ITEM", SqlKind.WITH_ITEM, 0) {
        /** {@inheritDoc} */
        @Override public SqlCall createCall(SqlLiteral qualifier, SqlParserPos pos, SqlNode... operands) {
            assert qualifier == null;
            assert operands.length == 5;

            return new IgniteSqlWithItem(pos, (SqlIdentifier)operands[0], (SqlNodeList)operands[1],
                operands[2], (SqlLiteral)operands[3], (SqlCycleClause)operands[4]);
        }

        /** {@inheritDoc} */
        @Override public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
            IgniteSqlWithItem item = (IgniteSqlWithItem)call;

            item.name.unparse(writer, getLeftPrec(), getRightPrec());

            if (item.columnList != null)
                item.columnList.unparse(writer, getLeftPrec(), getRightPrec());

            writer.keyword("AS");
            item.query.unparse(writer, MDX_PRECEDENCE, MDX_PRECEDENCE);

            if (item.cycleClause != null)
                item.cycleClause.unparse(writer, 0, 0);
        }
    };

    /** Cycle detection clause, removed after rewriting. */
    public @Nullable SqlCycleClause cycleClause;

    /** */
    public IgniteSqlWithItem(SqlParserPos pos, SqlIdentifier name, @Nullable SqlNodeList columnList,
        SqlNode query, SqlLiteral recursive, @Nullable SqlCycleClause cycleClause) {
        super(pos, name, columnList, query, recursive);

        this.cycleClause = cycleClause;
    }

    /** {@inheritDoc} */
    @Override public SqlOperator getOperator() {
        return OPERATOR;
    }

    /** {@inheritDoc} */
    @Override public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(name, columnList, query, recursive, cycleClause);
    }

    /** {@inheritDoc} */
    @Override public void setOperand(int i, @Nullable SqlNode operand) {
        if (i == 4)
            cycleClause = (SqlCycleClause)operand;
        else
            super.setOperand(i, operand);
    }
}
