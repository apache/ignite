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

import org.h2.util.*;
import org.jetbrains.annotations.*;

/**
 * Join of two tables or subqueries.
 */
public class GridSqlJoin extends GridSqlElement {
    /**
     * @param leftTbl Left table.
     * @param rightTbl Right table.
     */
    public GridSqlJoin(GridSqlElement leftTbl, GridSqlElement rightTbl) {
        addChild(leftTbl);
        addChild(rightTbl);
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
     * @return {@code ON} Condition.
     */
    @Nullable public GridSqlElement on() {
        return child(2);
    }

    /**
     * @return {@code true} If it is a {@code LEFT JOIN}.
     */
    public boolean leftJoin() {
        return false; // TODO
    }

    /** {@inheritDoc} */
    @Override public String getSQL() {
        StatementBuilder buff = new StatementBuilder();

        buff.append(leftTable().getSQL());

        buff.append(leftJoin() ? " \n LEFT JOIN " : " \n INNER JOIN ");

        buff.append(rightTable().getSQL());

        return buff.toString();
    }
}
