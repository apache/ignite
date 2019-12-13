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

package org.apache.ignite.internal.processors.query.calcite.serialize.expression;

import java.util.List;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;

/**
 *
 */
public class CallExpression implements Expression {
    public final String opName;
    public final SqlSyntax opSyntax;
    public final List<Expression> operands;

    public CallExpression(SqlOperator op, List<Expression> operands) {
        this.operands = operands;
        opName = op.getName();
        opSyntax = op.getSyntax();
    }

    @Override public <T> T implement(ExpImplementor<T> implementor) {
        return implementor.implement(this);
    }
}
