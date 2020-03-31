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
import java.util.List;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AggCallExp;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.type.DataType;

/**
 *
 */
public class AggregatePhysicalRel implements PhysicalRel {
    /** */
    public static final byte SINGLE = 0;

    /** */
    public static final byte MAP = 1;

    /** */
    public static final byte REDUCE = 2;

    /** */
    private byte type;

    /** */
    private List<ImmutableBitSet> groupSets;

    /** */
    private List<AggCallExp> calls;

    /** */
    private PhysicalRel input;

    /** */
    private DataType inputRowType;

    public AggregatePhysicalRel(byte type, List<ImmutableBitSet> groupSets, List<AggCallExp> calls, PhysicalRel input, DataType inputRowType) {
        this.type = type;
        this.groupSets = groupSets;
        this.calls = calls;
        this.input = input;
        this.inputRowType = inputRowType;
    }

    public AggregatePhysicalRel() {
    }

    public byte type() {
        return type;
    }

    public List<ImmutableBitSet> groupSets() {
        return groupSets;
    }

    public List<AggCallExp> calls() {
        return calls;
    }

    public PhysicalRel input() {
        return input;
    }

    public DataType inputRowType() {
        return inputRowType;
    }

    @Override public <T> T accept(PhysicalRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeByte(type);
        out.writeObject(groupSets);
        out.writeObject(calls);
        out.writeObject(input);
        if (type != REDUCE)
            out.writeObject(inputRowType);
    }

    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        type = in.readByte();
        groupSets = (List<ImmutableBitSet>) in.readObject();
        calls = (List<AggCallExp>) in.readObject();
        input = (PhysicalRel) in.readObject();
        if (type != REDUCE)
            inputRowType = (DataType) in.readObject();
    }
}
