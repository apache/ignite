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

package org.apache.ignite.internal.processors.query.calcite.serialize;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.Expression;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.type.DataType;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteFilter;

/**
 * Describes {@link IgniteFilter}
 */
public class FilterPhysicalRel implements PhysicalRel {
    /** */
    private DataType rowType;

    /** */
    private Expression condition;

    /** */
    private PhysicalRel input;

    /** */
    public FilterPhysicalRel() {
    }

    /** */
    public FilterPhysicalRel(DataType rowType, Expression condition, PhysicalRel input) {
        this.rowType = rowType;
        this.condition = condition;
        this.input = input;
    }

    /** */
    public DataType rowType() {
        return rowType;
    }

    /** */
    public Expression condition() {
        return condition;
    }

    /** */
    public PhysicalRel input() {
        return input;
    }

    /** {@inheritDoc} */
    @Override public <T> T accept(PhysicalRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(rowType);
        out.writeObject(condition);
        out.writeObject(input);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        rowType = (DataType) in.readObject();
        condition = (Expression) in.readObject();
        input = (PhysicalRel) in.readObject();
    }
}
