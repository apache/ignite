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
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.h2.command.Parser;

/**
 * Alias for column or table.
 */
public class GridSqlAlias extends GridSqlElement {
    /** */
    private final String alias;

    /** */
    private final boolean useAs;

    /**
     * @param alias Alias.
     * @param expr Expr.
     */
    public GridSqlAlias(String alias, GridSqlAst expr) {
        this(alias, expr, false);
    }

    /**
     * @param alias Alias.
     * @param expr Expr.
     * @param useAs Use 'AS' keyword.
     */
    public GridSqlAlias(String alias, GridSqlAst expr, boolean useAs) {
        super(new ArrayList<GridSqlAst>(1));

        addChild(expr);

        assert !F.isEmpty(alias) : alias;

        this.useAs = useAs;
        this.alias = alias;
    }

    /**
     * @param el Element.
     * @return Unwrapped from alias element.
     */
    @SuppressWarnings("unchecked")
    public static <X extends GridSqlAst> X unwrap(GridSqlAst el) {
        el = el instanceof GridSqlAlias ? el.child() : el;

        assert el != null;

        return (X)el;
    }

    /** {@inheritDoc} */
    @Override public String getSQL() {
        SB b = new SB();

        GridSqlAst child = child(0);

        boolean tbl = child instanceof GridSqlTable;

        b.a(tbl ? ((GridSqlTable)child).getBeforeAliasSql() : child.getSQL());

        b.a(useAs ? " AS " : " ");
        b.a(Parser.quoteIdentifier(alias));

        if (tbl)
            b.a(((GridSqlTable)child).getAfterAliasSQL());

        return b.toString();
    }

    /**
     * @return Alias.
     */
    public String alias() {
        return alias;
    }
}
