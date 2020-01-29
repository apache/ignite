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

package org.apache.ignite.internal.processors.query.calcite.serialize.expression;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.processors.query.calcite.serialize.type.DataType;
import org.apache.ignite.internal.processors.query.calcite.serialize.type.Types;

/**
 * Describes {@link org.apache.calcite.rex.RexLocalRef}.
 */
public class LocalRefExpression implements Expression {
    /** */
    private final DataType type;

    /** */
    private final int index;

    /**
     * @param type Data type.
     * @param index Index.
     */
    public LocalRefExpression(RelDataType type, int index) {
        this.type = Types.fromType(type);
        this.index = index;
    }

    /**
     * @return Data type.
     */
    public DataType dataType() {
        return type;
    }

    /**
     * @return Index.
     */
    public int index() {
        return index;
    }

    /** {@inheritDoc} */
    @Override public <T> T implement(ExpImplementor<T> implementor) {
        return implementor.implement(this);
    }
}
