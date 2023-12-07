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
import java.util.Objects;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.ignite.internal.processors.query.calcite.sql.IgniteSqlKill;

/**
 * Parse tree for {@code KILL SCAN} statement.
 */
public class IgniteSqlKillScanQuery extends IgniteSqlKill {
    /** */
    private static final SqlOperator OPERATOR = new SqlSpecialOperator("KILL SCAN", SqlKind.OTHER_DDL);

    /**  */
    private final SqlCharStringLiteral nodeId;

    /** */
    private final SqlCharStringLiteral cacheName;

    /** */
    private final SqlNumericLiteral qryId;

    /** */
    public IgniteSqlKillScanQuery(
        SqlParserPos pos,
        SqlCharStringLiteral nodeId,
        SqlCharStringLiteral cacheName,
        SqlNumericLiteral qryId
    ) {
        super(OPERATOR, pos);
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId");
        this.cacheName = Objects.requireNonNull(cacheName, "cacheName");
        this.qryId = Objects.requireNonNull(qryId, "qryId");

    }

    /** {@inheritDoc} */
    @Override public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(nodeId, cacheName, qryId);
    }

    /** {@inheritDoc} */
    @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword(getOperator().getName());
        nodeId.unparse(writer, 0, 0);
        cacheName.unparse(writer, 0, 0);
        qryId.unparse(writer, 0, 0);
    }

    /**
     * @return Node id.
     */
    public SqlCharStringLiteral nodeId() {
        return nodeId;
    }

    /**
     * @return Cache name.
     */
    public SqlCharStringLiteral cacheName() {
        return cacheName;
    }

    /**
     * @return Query id.
     */
    public SqlNumericLiteral queryId() {
        return qryId;
    }
}
