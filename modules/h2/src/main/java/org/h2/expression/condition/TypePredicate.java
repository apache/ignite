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

/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.condition;

import java.util.Arrays;

import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionVisitor;
import org.h2.expression.ValueExpression;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueNull;

/**
 * Type predicate (IS [NOT] OF).
 */
public class TypePredicate extends Condition {

    private Expression left;
    private final boolean not;
    private final TypeInfo[] typeList;
    private int[] valueTypes;

    public TypePredicate(Expression left, boolean not, TypeInfo[] typeList) {
        this.left = left;
        this.not = not;
        this.typeList = typeList;
    }

    @Override
    public StringBuilder getSQL(StringBuilder builder, boolean alwaysQuote) {
        builder.append('(');
        left.getSQL(builder, alwaysQuote).append(" IS");
        if (not) {
            builder.append(" NOT");
        }
        builder.append(" OF (");
        for (int i = 0; i < typeList.length; i++) {
            if (i > 0) {
                builder.append(", ");
            }
            typeList[i].getSQL(builder);
        }
        return builder.append(')');
    }

    @Override
    public Expression optimize(Session session) {
        left = left.optimize(session);
        int count = typeList.length;
        valueTypes = new int[count];
        for (int i = 0; i < count; i++) {
            valueTypes[i] = typeList[i].getValueType();
        }
        Arrays.sort(valueTypes);
        if (left.isConstant()) {
            return ValueExpression.get(getValue(session));
        }
        return this;
    }

    @Override
    public Value getValue(Session session) {
        Value l = left.getValue(session);
        if (l == ValueNull.INSTANCE) {
            return ValueNull.INSTANCE;
        }
        return ValueBoolean.get(Arrays.binarySearch(valueTypes, l.getValueType()) >= 0 ^ not);
    }

    @Override
    public Expression getNotIfPossible(Session session) {
        return new TypePredicate(left, !not, typeList);
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        left.setEvaluatable(tableFilter, b);
    }

    @Override
    public void updateAggregate(Session session, int stage) {
        left.updateAggregate(session, stage);
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level, int state) {
        left.mapColumns(resolver, level, state);
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        return left.isEverything(visitor);
    }

    @Override
    public int getCost() {
        return left.getCost() + 1;
    }

    @Override
    public int getSubexpressionCount() {
        return 1;
    }

    @Override
    public Expression getSubexpression(int index) {
        if (index == 0) {
            return left;
        }
        throw new IndexOutOfBoundsException();
    }

}
