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

import java.util.ArrayList;
import org.h2.util.StatementBuilder;
import org.h2.util.StringUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Join of two tables or subqueries.
 */
public class GridSqlJoin extends GridSqlElement {
    /** */
    private boolean leftOuter;

    /**
     * @param leftTbl Left table.
     * @param rightTbl Right table.
     * @param leftOuter Left outer join.
     * @param on Join condition.
     */
    public GridSqlJoin(GridSqlElement leftTbl, GridSqlElement rightTbl, boolean leftOuter, @Nullable GridSqlElement on) {
        super(new ArrayList<GridSqlElement>(on == null ? 2 : 3));

        addChild(leftTbl);
        addChild(rightTbl);

        if (on != null)
            addChild(on);

        this.leftOuter = leftOuter;
    }

    /**
     * @return Table 1.
     */
    public GridSqlElement leftTable() {
        return child(0);
    }

    /**
     * @return Table 2.
     */
    public GridSqlElement rightTable() {
        return child(1);
    }

    /**
     * @return {@code JOIN ON} condition.
     */
    @Nullable public GridSqlElement on() {
        return size() < 3 ? null : child(2);
    }

    /** {@inheritDoc} */
    @Override public String getSQL() {
        StatementBuilder buff = new StatementBuilder();

        buff.append(leftTable().getSQL());

        buff.append(leftOuter ? " \n LEFT OUTER JOIN " : " \n INNER JOIN ");

        buff.append(rightTable().getSQL());

        GridSqlElement on = on();

        if (on != null)
            buff.append(" \n ON ").append(StringUtils.unEnclose(on.getSQL()));

        return buff.toString();
    }
}