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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.processors.query.calcite.serialize.type.DataType;

/**
 *
 */
public class InputRefExpression implements Expression {
    public final DataType type;
    public final int index;

    public InputRefExpression(RelDataType type, int index) {
        this.type = DataType.fromType(type);
        this.index = index;
    }

    @Override public <T> T implement(ExpImplementor<T> implementor) {
        return implementor.implement(this);
    }
}
