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

import org.apache.ignite.compatibility.sql.randomsql.Operator;

import static org.apache.ignite.compatibility.sql.randomsql.ast.AstUtils.r100;

/**
 *
 */
public class Comparison extends BooleanExpression{
    /** */
    private final Expression left;

    /** */
    private final Expression right;

    /** */
    private final Operator op;

    /** */
    protected Comparison(Ast parent) {
        super(parent);

        Class<?> opsType = r100(parent, 80) ? Integer.class : Boolean.class;

        left = Expression.createRandom(parent, opsType);
        right = Expression.createRandom(parent, opsType);
        op = parent.scope().pickRandomOp(opsType, opsType, Boolean.class);
    }

    /** {@inheritDoc} */
    @Override public void print(StringBuilder out) {
        out.append("(");
        left.print(out);
        out.append(" ")
            .append(op.name())
            .append(" ");
        right.print(out);
        out.append(")");
    }
}
