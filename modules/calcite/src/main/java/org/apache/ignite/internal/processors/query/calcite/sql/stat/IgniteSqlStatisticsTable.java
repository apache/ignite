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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.ignite.internal.util.typedef.F;
import org.checkerframework.checker.nullness.qual.Nullable;

/** */
public class IgniteSqlStatisticsTable extends SqlCall {
    /** */
    private static final SqlOperator OPERATOR =
        new SqlSpecialOperator("StatisticsTable", SqlKind.OTHER);

    /** */
    private final SqlIdentifier name;

    /** */
    private final SqlNodeList columns;

    /** */
    public IgniteSqlStatisticsTable(
        SqlIdentifier name,
        @Nullable SqlNodeList columns,
        SqlParserPos pos
    ) {
        super(pos);
        this.name = name;
        this.columns = F.isEmpty(columns)
            ? SqlNodeList.EMPTY
            : SqlNodeList.of(columns.getParserPosition(), columns.getList());
    }

    /** {@inheritDoc} */
    @Override public SqlOperator getOperator() {
        return OPERATOR;
    }

    /** {@inheritDoc} */
    @Override public List<SqlNode> getOperandList() {
        return ImmutableList.of(name, columns);
    }

    /** {@inheritDoc} */
    @Override public SqlNode clone(SqlParserPos pos) {
        return new IgniteSqlStatisticsTable(name, columns, pos);
    }

    /** {@inheritDoc} */
    @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        name.unparse(writer, leftPrec, rightPrec);
        if (columns != null)
            columns.unparse(writer, leftPrec, rightPrec);
    }

    /**
     * @return Name of the table.
     */
    public SqlIdentifier name() {
        return name;
    }

    /**
     * @return List of the specified columns.
     */
    public List<SqlNode> columns() {
        if (columns == null)
            return Collections.emptyList();

        return new ArrayList<>(columns.getList());
    }
}
