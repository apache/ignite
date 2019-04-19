/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.h2.sql;

import java.util.ArrayList;


/**
 * Subquery expression.
 */
public class GridSqlSubquery extends GridSqlElement {
    /**
     * @param subQry Subquery.
     */
    public GridSqlSubquery(GridSqlQuery subQry) {
        super(new ArrayList<GridSqlAst>(1));

        addChild(subQry);
    }

    /** {@inheritDoc} */
    @Override public String getSQL() {
        return "(" + subquery().getSQL() + ")";
    }

    /**
     * @return Subquery AST.
     */
    public <X extends GridSqlQuery> X subquery() {
        return child(0);
    }
}