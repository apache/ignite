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

package org.apache.ignite.internal.processors.query.h2.sql;

import org.apache.ignite.internal.util.typedef.F;
import org.h2.util.StatementBuilder;

import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlFunctionType.AVG;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlFunctionType.COUNT;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlFunctionType.COUNT_ALL;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlFunctionType.GROUP_CONCAT;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlFunctionType.MAX;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlFunctionType.MIN;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlFunctionType.SUM;

/**
 * Aggregate function.
 */
public class GridSqlAggregateFunction extends GridSqlFunction {
    /** */
    private static final GridSqlFunctionType[] TYPE_INDEX = new GridSqlFunctionType[] {
        COUNT_ALL, COUNT, GROUP_CONCAT, SUM, MIN, MAX, AVG,
//        STDDEV_POP, STDDEV_SAMP, VAR_POP, VAR_SAMP, BOOL_OR, BOOL_AND, SELECTIVITY, HISTOGRAM,
    };

    /** */
    private final boolean distinct;

    /** */
    private GridSqlElement groupConcatSeparator;

    /** */
    private GridSqlElement[] groupConcatOrderExpression;

    /** */
    private boolean[] groupConcatOrderDesc;

    /**
     * @param distinct Distinct.
     * @param type Type.
     */
    public GridSqlAggregateFunction(boolean distinct, GridSqlFunctionType type) {
        super(type);

        this.distinct = distinct;
    }

    /**
     * @param distinct Distinct.
     * @param typeId Type.
     */
    public GridSqlAggregateFunction(boolean distinct, int typeId) {
        this(distinct, TYPE_INDEX[typeId]);
    }

    /**
     * Checks if the aggregate type is valid.
     *
     * @param typeId Aggregate type id.
     * @return True is valid, otherwise false.
     */
    protected static boolean isValidType(int typeId) {
        return (typeId >= 0) && (typeId < TYPE_INDEX.length);
    }

    /**
     * @return Distinct.
     */
    public boolean distinct() {
        return distinct;
    }

    /**
     * @param groupConcatOrderExpression Order expression.
     * @param groupConcatOrderDesc Order descending flag.
     */
    public void setOder(GridSqlElement[] groupConcatOrderExpression, boolean[] groupConcatOrderDesc) {
        this.groupConcatOrderExpression = groupConcatOrderExpression;
        this.groupConcatOrderDesc = groupConcatOrderDesc;
    }

    /**
     * @param groupConcatSeparator Separator expression.
     */
    public void setGroupConcatSeparator(GridSqlElement groupConcatSeparator) {
        this.groupConcatSeparator = groupConcatSeparator;
    }

    /** {@inheritDoc} */
    @Override public String getSQL() {
        if (type == COUNT_ALL)
            return "COUNT(*)";

        StatementBuilder buff = new StatementBuilder(name() + "(");

        if (distinct)
            buff.append("DISTINCT ");

        buff.append(child().getSQL());

        if (!F.isEmpty(groupConcatOrderExpression)) {
            buff.append(" ORDER BY ");

            for (int i = 0; i < groupConcatOrderExpression.length; ++i) {
                buff.appendExceptFirst(", ");

                buff.append(groupConcatOrderExpression[i].getSQL());

                if (groupConcatOrderDesc[i])
                    buff.append(" DESC");
            }
        }

        if (groupConcatSeparator != null)
            buff.append(" SEPARATOR " + groupConcatSeparator.getSQL());

        buff.append(')');

        return buff.toString();
    }
}