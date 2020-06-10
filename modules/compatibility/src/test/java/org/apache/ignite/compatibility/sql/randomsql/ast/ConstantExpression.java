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
import static org.apache.ignite.compatibility.sql.randomsql.ast.AstUtils.rAsString;

/**
 * TODO: Add class description.
 */
public class ConstantExpression extends Expression {
    private final String constant;

    protected ConstantExpression(Ast parent, Class<?> type, String constant) {
        super(parent, type);
        this.constant = constant;
    }

    @Override public void print(StringBuilder out) {
        out.append(" ").append(constant).append(" ");
    }

    public static ConstantExpression createRandom(Ast parent, Class<?> typeConstraint) {
        if (typeConstraint == Integer.class)
            return new ConstantExpression(parent, typeConstraint, rAsString(parent, 100));
        else if (typeConstraint == Boolean.class)
            return new ConstantExpression(parent, typeConstraint, r(parent, 2) == 0 ? "true" : "false");
        else
            throw new  AssertionError("Not supported type constraint:" + typeConstraint);
    }
}
