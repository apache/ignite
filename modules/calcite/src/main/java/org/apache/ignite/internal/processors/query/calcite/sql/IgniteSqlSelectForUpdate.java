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
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;
import org.jetbrains.annotations.Nullable;

/**
 * Parse tree for {@code SELECT ... FOR UPDATE [OF col [, col ...]] [WAIT n | NOWAIT]} statement.
 *
 * <p>The {@code FOR UPDATE} clause requests pessimistic row-level locks on the rows returned
 * by the query. Actual lock acquisition is not yet supported; this node exists to reserve the
 * syntax and produce a clear "not yet supported" error at query-preparation time.
 *
 * <p>Fields:
 * <ul>
 *     <li>{@code query} — the underlying SELECT/WITH/VALUES query</li>
 *     <li>{@code ofList} — optional list of column references identifying which tables to lock;
 *         {@code null} means all tables referenced in FROM</li>
 *     <li>{@code waitSeconds} — {@code null} = use transaction timeout,
 *         {@code 0} = NOWAIT, positive = WAIT n seconds</li>
 * </ul>
 */
public class IgniteSqlSelectForUpdate extends SqlCall {
    /** */
    private static final SqlOperator OPERATOR =
        new SqlSpecialOperator("SELECT FOR UPDATE", SqlKind.OTHER);

    /** The wrapped SELECT / WITH / VALUES query. */
    private final SqlNode query;

    /**
     * Columns from the {@code OF} list, or {@code null} when not specified (lock all tables).
     * Each element is a {@link org.apache.calcite.sql.SqlIdentifier} referencing a table column.
     */
    @Nullable private final SqlNodeList ofList;

    /**
     * Lock-wait limit: {@code null} = use transaction timeout, {@code 0} = NOWAIT,
     * positive = WAIT n seconds.
     */
    @Nullable private final Long waitSeconds;

    /** */
    public IgniteSqlSelectForUpdate(
        SqlParserPos pos,
        SqlNode query,
        @Nullable SqlNodeList ofList,
        @Nullable Long waitSeconds
    ) {
        super(pos);

        this.query = query;
        this.ofList = ofList;
        this.waitSeconds = waitSeconds;
    }

    /** {@inheritDoc} */
    @Override public SqlOperator getOperator() {
        return OPERATOR;
    }

    /** {@inheritDoc} */
    @Override public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(query, ofList);
    }

    /** Returns the wrapped query (SELECT / WITH / VALUES). */
    public SqlNode query() {
        return query;
    }

    /**
     * Returns the {@code OF} column list, or {@code null} when not specified (lock all tables).
     */
    @Nullable public SqlNodeList ofList() {
        return ofList;
    }

    /**
     * Returns the wait limit: {@code null} = transaction timeout, {@code 0} = NOWAIT,
     * positive = WAIT n seconds.
     */
    @Nullable public Long waitSeconds() {
        return waitSeconds;
    }

    /** {@inheritDoc} */
    @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        query.unparse(writer, leftPrec, rightPrec);
        writer.keyword("FOR");
        writer.keyword("UPDATE");

        if (ofList != null) {
            writer.keyword("OF");
            ofList.unparse(writer, 0, 0);
        }

        if (waitSeconds != null) {
            if (waitSeconds == 0L)
                writer.keyword("NOWAIT");
            else {
                writer.keyword("WAIT");
                writer.print(String.valueOf(waitSeconds));
            }
        }
    }
}
