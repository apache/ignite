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
package org.apache.ignite.internal.processors.query.calcite.sql.stat;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

/** */
public abstract class IgniteSqlStatisticsCommand extends SqlDdl {
    /** */
    protected final SqlNodeList tables;

    /** */
    protected IgniteSqlStatisticsCommand(
        SqlOperator operator,
        SqlNodeList tables,
        SqlParserPos pos
    ) {
        super(operator, pos);

        this.tables = SqlNodeList.of(tables.getParserPosition(), tables.getList());
    }

    /** {@inheritDoc} */
    @Override public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(tables);
    }

    /** {@inheritDoc} */
    @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword(getOperator().getName());

        tables.unparse(writer, leftPrec, rightPrec);
    }

    /**
     * @return List of tables.
     */
    public List<IgniteSqlStatisticsTable> tables() {
        if (tables == null)
            return Collections.emptyList();

        return tables.stream().map(IgniteSqlStatisticsTable.class::cast).collect(Collectors.toList());
    }
}
