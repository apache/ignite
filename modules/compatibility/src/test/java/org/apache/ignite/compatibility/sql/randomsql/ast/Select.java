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
package org.apache.ignite.compatibility.sql.randomsql.ast;

import org.apache.ignite.compatibility.sql.randomsql.Scope;

/**
 * TODO: Add class description.
 */
public class Select extends Ast {
    private Ast from;

    private Expression where; // TODO bool expr

    public Select(Ast parent) {
        super(parent);
    }

    public Select(Scope scope) {
        super(scope);
    }

    @Override public void print(StringBuilder out) {
        out.append("SELECT * FROM ");
        from.print(out);
        out.append(" WHERE ");
        where.print(out);
        out.append(" LIMIT  99000");
    }

    public static Select createParentRandom(Scope initScope) {
        Select select = new Select(initScope);
        select.from = new From(select);
        select.where = BooleanExpression.createRandom(select, Integer.class);
        return select;
    }
}
