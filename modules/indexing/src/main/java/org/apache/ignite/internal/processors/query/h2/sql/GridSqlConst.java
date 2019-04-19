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

import java.util.Collections;
import org.h2.expression.ValueExpression;
import org.h2.value.Value;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueNull;

/**
 * Constant value.
 */
public class GridSqlConst extends GridSqlElement {
    /** */
    public static final GridSqlElement NULL = new GridSqlConst(ValueNull.INSTANCE)
        .resultType(GridSqlType.fromExpression(ValueExpression.getNull()));

    /** */
    public static final GridSqlConst TRUE = (GridSqlConst)new GridSqlConst(ValueBoolean.get(true))
        .resultType(GridSqlType.BOOLEAN);

    /** */
    private final Value val;

    /**
     * @param val Value.
     */
    public GridSqlConst(Value val) {
        super(Collections.<GridSqlAst>emptyList());

        this.val = val;
    }

    /**
     * @return Value.
     */
    public Value value() {
        return val;
    }

    /** {@inheritDoc} */
    @Override public String getSQL() {
        return val.getSQL();
    }
}