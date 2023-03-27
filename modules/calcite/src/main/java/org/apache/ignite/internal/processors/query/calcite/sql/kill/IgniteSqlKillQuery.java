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
package org.apache.ignite.internal.processors.query.calcite.sql.kill;

import java.util.List;
import java.util.UUID;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.ignite.internal.processors.query.calcite.sql.IgniteSqlKill;

/** */
public class IgniteSqlKillQuery extends IgniteSqlKill {
    /** */
    private static final SqlOperator SYNC_OPERATOR = new SqlSpecialOperator("KILL QUERY", SqlKind.OTHER_DDL);

    /** */
    private static final SqlOperator ASYNC_OPERATOR = new SqlSpecialOperator("KILL QUERY ASYNC", SqlKind.OTHER_DDL);

    /** Global queryId literal. */
    private final SqlCharStringLiteral globalQueryId;

    /** Node id. */
    private final UUID nodeId;

    /** Local query id. */
    private final long queryId;

    /** Command async. */
    private final boolean isAsync;

    /** */
    public IgniteSqlKillQuery(
        SqlParserPos pos,
        SqlCharStringLiteral globalQueryId,
        UUID nodeId,
        long queryId,
        boolean isAsync
    ) {
        super(isAsync ? ASYNC_OPERATOR : SYNC_OPERATOR, pos);

        this.globalQueryId = globalQueryId;
        this.nodeId = nodeId;
        this.queryId = queryId;
        this.isAsync = isAsync;
    }

    /** {@inheritDoc} */
    @Override public List<SqlNode> getOperandList() {
        return ImmutableList.of(globalQueryId);
    }

    /** {@inheritDoc} */
    @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword(getOperator().getName());
        globalQueryId.unparse(writer, 0, 0);
    }

    /**
     * @return Node id.
     */
    public UUID nodeId() {
        return nodeId;
    }

    /**
     * @return Local query id.
     */
    public long queryId() {
        return queryId;
    }

    /**
     * @return Async flag.
     */
    public boolean isAsync() {
        return isAsync;
    }
}
