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

import org.h2.util.StringUtils;

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
    private static final GridSqlFunctionType[] TYPE_INDEX = new GridSqlFunctionType[]{
        COUNT_ALL, COUNT, GROUP_CONCAT, SUM, MIN, MAX, AVG,
//        STDDEV_POP, STDDEV_SAMP, VAR_POP, VAR_SAMP, BOOL_OR, BOOL_AND, SELECTIVITY, HISTOGRAM,
    };

    /** */
    private final boolean distinct;

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
     * @return Distinct.
     */
    public boolean distinct() {
        return distinct;
    }

    /** {@inheritDoc} */
    @Override public String getSQL() {
        String text;

        switch (type) {
            case GROUP_CONCAT:
                throw new UnsupportedOperationException();

            case COUNT_ALL:
                return "COUNT(*)";

            default:
                text = type.name();

                break;
        }

        if (distinct)
            return text + "(DISTINCT " + child().getSQL() + ")";

        return text + StringUtils.enclose(child().getSQL());
    }
}