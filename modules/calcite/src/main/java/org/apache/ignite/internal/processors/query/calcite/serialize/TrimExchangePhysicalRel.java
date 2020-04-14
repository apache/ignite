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
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionFunction;

/**
 *
 */
public class TrimExchangePhysicalRel implements PhysicalRel {
    /** */
    private DistributionFunction function;

    /** */
    private ImmutableIntList keys;

    /** */
    private int partitions;

    /** */
    private PhysicalRel input;

    /** */
    public TrimExchangePhysicalRel() {
    }

    /** */
    public TrimExchangePhysicalRel(DistributionFunction function, ImmutableIntList keys, int partitions, PhysicalRel input) {
        this.function = function;
        this.keys = keys;
        this.partitions = partitions;
        this.input = input;
    }

    /** */
    public DistributionFunction function() {
        return function;
    }

    /** */
    public ImmutableIntList distributionKeys() {
        return keys;
    }

    /** */
    public int partitions() {
        return partitions;
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
        out.writeObject(function);
        out.writeObject(keys.toIntArray());
        out.writeInt(partitions);
        out.writeObject(input);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        function = (DistributionFunction) in.readObject();
        keys = ImmutableIntList.of((int[]) in.readObject());
        partitions = in.readInt();
        input = (PhysicalRel) in.readObject();
    }
}
