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

import static org.apache.ignite.compatibility.sql.randomsql.ast.AstUtils.r;

/**
 * TODO: Add class description.
 */
public abstract class Expression extends Ast {

    private final Class<?> type;

    protected Expression(Ast parent, Class<?> type) {
        super(parent);

        this.type = type;
    }

    public static Expression createRandom(Ast parent, Class<?> typeConstraint) {
        if (r(parent, 10) < 2)
            return ConstantExpression.createRandom(parent, typeConstraint);
        else
            return ColumnRef.createRandom(parent, typeConstraint);
    }
}
