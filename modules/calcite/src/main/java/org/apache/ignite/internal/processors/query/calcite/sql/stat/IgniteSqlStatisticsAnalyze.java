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
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.ignite.internal.util.typedef.F;
import org.checkerframework.checker.nullness.qual.Nullable;

/** */
public class IgniteSqlStatisticsAnalyze extends IgniteSqlStatisticsCommand {
    /** */
    private static final SqlOperator OPERATOR = new SqlSpecialOperator("ANALYZE", SqlKind.OTHER_DDL);

    /** */
    private final SqlNodeList options;

    /** */
    public IgniteSqlStatisticsAnalyze(
        SqlNodeList tables,
        @Nullable SqlNodeList options,
        SqlParserPos pos
    ) {
        super(OPERATOR, tables, pos);
        if (options != null)
            this.options = SqlNodeList.of(options.getParserPosition(), options.getList());
        else
            this.options = null;
    }

    /** {@inheritDoc} */
    @Override public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(tables, options);
    }

    /** {@inheritDoc} */
    @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        super.unparse(writer, leftPrec, rightPrec);

        if (!F.isEmpty(options)) {
            writer.keyword("WITH");

            options.unparse(writer, 0, 0);
        }
    }

    /**
     * @return List of options.
     */
    public List<IgniteSqlStatisticsAnalyzeOption> options() {
        if (options == null)
            return Collections.emptyList();

        return options.stream().map(IgniteSqlStatisticsAnalyzeOption.class::cast).collect(Collectors.toList());
    }
}
