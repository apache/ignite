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

import org.apache.ignite.compatibility.sql.randomsql.Column;
import org.apache.ignite.compatibility.sql.randomsql.Scope;

/**
 *
 */
public class ColumnRef extends Expression {
    /** */
    private final String tblAlias;

    /** */
    private final Column col;

    /** */
    private final Class<?> typeCls;

    /** */
    protected ColumnRef(Ast parent, Column col, String tblAlias) {
        super(parent, col.typeClass());
        this.col = col;
        this.tblAlias = tblAlias;
        this.typeCls = col.typeClass();
    }

    @Override public void print(StringBuilder out) {
        out.append(" ")
            .append(tblAlias)
            .append(".")
            .append(col.name())
            .append(" ");
    }

    public static ColumnRef createRandom(Ast parent, Class<?> typeConstraint) {
        Scope scope = parent.scope();

        return scope.pickRandomColumn(typeConstraint);
    }

    public Class<?> typeClass() {
        return typeCls;  // TODO: implement.
    }
}
