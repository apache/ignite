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
    public static final int LEFT_TABLE_CHILD = 0;

    /** */
    public static final int RIGHT_TABLE_CHILD = 1;

    /** */
    public static final int ON_CHILD = 2;

    /** */
    private boolean leftOuter;

    /**
     * @param leftTbl Left table.
     * @param rightTbl Right table.
     * @param leftOuter Left outer join.
     * @param on Join condition.
     */
    public GridSqlJoin(GridSqlElement leftTbl, GridSqlElement rightTbl, boolean leftOuter, @Nullable GridSqlElement on) {
        super(new ArrayList<GridSqlAst>(3));

        addChild(leftTbl);
        addChild(rightTbl);

        if (on == null) // To avoid query nesting issues in FROM clause we need to always generate ON condition.
            on = GridSqlConst.TRUE;

        addChild(on);

        this.leftOuter = leftOuter;
    }

    /**
     * @return Left table.
     */
    public GridSqlElement leftTable() {
        return child(LEFT_TABLE_CHILD);
    }

    /**
     * @param tbl Right table to set.
     */
    public void leftTable(GridSqlElement tbl) {
        child(LEFT_TABLE_CHILD, tbl);
    }

    /**
     * @return Right table.
     */
    public GridSqlElement rightTable() {
        return child(RIGHT_TABLE_CHILD);
    }

    /**
     * @param tbl Right table to set.
     */
    public void rightTable(GridSqlElement tbl) {
        child(RIGHT_TABLE_CHILD, tbl);
    }

    /**
     * @return {@code JOIN ON} condition.
     */
    public GridSqlElement on() {
        return child(ON_CHILD);
    }

    /**
     * @return {@code true} If this is a LEFT OUTER JOIN.
     */
    public boolean isLeftOuter() {
        return leftOuter;
    }

    /** {@inheritDoc} */
    @Override public String getSQL() {
        StatementBuilder buff = new StatementBuilder();

        buff.append(leftTable().getSQL());

        buff.append(leftOuter ? " \n LEFT OUTER JOIN " : " \n INNER JOIN ");

        buff.append(rightTable().getSQL());

        buff.append(" \n ON ").append(StringUtils.unEnclose(on().getSQL()));

        return buff.toString();
    }
}
